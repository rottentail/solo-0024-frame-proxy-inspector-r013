from __future__ import annotations

from frame_proxy.config import Settings
from frame_proxy.domain.errors import (
    ApplicationError,
    CancellationError,
    ErrorCode,
    StateError,
)
from frame_proxy.domain.models import ContactSheetRecord
from frame_proxy.domain.policies import ContactSheetRequest, validate_identifier
from frame_proxy.domain.transitions import transition_contact_sheet
from frame_proxy.media.avi import probe_avi
from frame_proxy.media.contact import compose_grid, plan_grid
from frame_proxy.media.frames import Frame, decode_frame_at
from frame_proxy.media.ppm import parse_ppm_header
from frame_proxy.service.cancellation import CancellationRegistry, WorkflowGate
from frame_proxy.storage.repository import FileRepository

JOB_KIND = "contact-sheet"


class ContactSheetService:
    def __init__(
        self,
        repository: FileRepository,
        settings: Settings,
        *,
        registry: CancellationRegistry | None = None,
        gate: WorkflowGate | None = None,
    ) -> None:
        self._repository = repository
        self._settings = settings
        self._registry = registry or CancellationRegistry()
        self._gate = gate or WorkflowGate()

    def create(
        self,
        inspection_id: str,
        request: ContactSheetRequest,
    ) -> ContactSheetRecord:
        normalized_id = validate_identifier(inspection_id, "inspection_id")
        inspection = self._repository.get_inspection(normalized_id)
        if inspection.state != "indexed":
            raise StateError(
                "contact sheets require an indexed inspection",
                details={"state": inspection.state},
            )
        clip = self._repository.get_clip(inspection.clip_id)
        plan = plan_grid(
            scene_count=len(inspection.scenes),
            source_width=clip.width,
            source_height=clip.height,
            request=request,
            max_scenes=self._settings.max_sheet_scenes,
            max_pixels=self._settings.max_sheet_pixels,
        )

        record = ContactSheetRecord(
            sheet_id=request.sheet_id,
            inspection_id=inspection.inspection_id,
            state="planned",
            columns=request.columns,
            tile_width=request.tile_width,
            tile_height=plan.tile_height,
            gutter=request.gutter,
            background_rgb=request.background_rgb,
            selection=request.selection,
            rows=plan.rows,
            image_width=plan.image_width,
            image_height=plan.image_height,
            artifact_name="sheet.ppm",
            artifact_digest=None,
            tile_map=[],
        )
        self._repository.create_contact_sheet(record)

        token = self._registry.token(
            JOB_KIND,
            record.sheet_id,
            durable_checkpoint=lambda: self._repository.cancellation_marker_exists(
                JOB_KIND,
                record.sheet_id,
            ),
        )
        token.checkpoint(stage="creation")
        transition_contact_sheet(record, "compositing", reason="layout reserved")
        self._repository.update_contact_sheet(record)

        try:
            media = self._repository.get_clip_media(clip.clip_id)
            descriptor = probe_avi(media)
            frames: dict[int, Frame] = {}
            for progress, scene in enumerate(inspection.scenes):
                token.checkpoint(stage="frame decode")
                self._gate.await_gate(
                    JOB_KIND,
                    record.sheet_id,
                    progress=progress,
                )
                if request.selection == "first":
                    frame_index = scene.start_frame
                elif request.selection == "midpoint":
                    frame_index = scene.start_frame + (
                        scene.end_frame - scene.start_frame
                    ) // 2
                else:
                    frame_index = scene.representative_frame
                frames[frame_index] = decode_frame_at(
                    media,
                    descriptor,
                    frame_index,
                )
            token.checkpoint(stage="composition")
            result = compose_grid(
                plan=plan,
                scenes=tuple(inspection.scenes),
                frames=frames,
                background_rgb=request.background_rgb,
                selection=request.selection,
                checkpoint=lambda: token.checkpoint(stage="tile composition"),
            )
            header = parse_ppm_header(result.payload)
            if (
                header.width != record.image_width
                or header.height != record.image_height
            ):
                raise ValueError("composed PPM dimensions disagree with the plan")
            record.artifact_digest = result.digest
            record.tile_map = list(result.tile_map)
            token.checkpoint(stage="publication")
            settled = self._settle(record, "ready", artifact=result.payload)
            self._raise_if_cancelled(settled)
            return settled
        except CancellationError:
            settled = self._settle_cancelled(record)
            raise self._cancelled_response(settled)
        except ApplicationError as exc:
            settled = self._settle_failure(record, exc.code.value, exc.message)
            self._raise_if_cancelled(settled, original=exc)
            raise
        except Exception as exc:
            settled = self._settle_failure(
                record,
                ErrorCode.INTERNAL.value,
                str(exc),
            )
            self._raise_if_cancelled(settled)
            raise ApplicationError(
                ErrorCode.INTERNAL,
                "contact sheet composition failed",
                status=500,
            ) from exc
        finally:
            self._registry.discard(JOB_KIND, record.sheet_id)

    def cancel(self, sheet_id: str) -> ContactSheetRecord:
        normalized = validate_identifier(sheet_id, "sheet_id")
        record = self._repository.get_contact_sheet(normalized)
        # Durable marker first: a worker in another process must see the
        # decision before the state record changes.
        self._repository.write_cancellation_marker(
            JOB_KIND,
            normalized,
            reason="client requested cancellation",
        )
        self._registry.signal(JOB_KIND, normalized)
        record.error_code = ErrorCode.OPERATION_CANCELLED.value
        record.error_message = "contact sheet composition was cancelled before readiness"
        return self._settle(record, "cancelled")

    def _settle(
        self,
        record: ContactSheetRecord,
        terminal_state: str,
        *,
        artifact: bytes | None = None,
    ) -> ContactSheetRecord:
        try:
            return self._repository.settle_contact_sheet(
                record,
                terminal_state,
                artifact=artifact,
            )
        except ApplicationError:
            raise
        except Exception as exc:
            raise ApplicationError(
                ErrorCode.STORAGE,
                "contact sheet state could not be committed",
                status=500,
            ) from exc

    def _settle_cancelled(self, record: ContactSheetRecord) -> ContactSheetRecord:
        record.error_code = ErrorCode.OPERATION_CANCELLED.value
        record.error_message = "contact sheet composition was cancelled before readiness"
        return self._settle(record, "cancelled")

    def _settle_failure(
        self,
        record: ContactSheetRecord,
        code: str,
        message: str,
    ) -> ContactSheetRecord:
        record.error_code = code
        record.error_message = message
        try:
            return self._settle(record, "failed")
        except ApplicationError:
            return self._repository.get_contact_sheet(record.sheet_id)

    def _cancelled_response(self, record: ContactSheetRecord) -> CancellationError:
        return CancellationError(
            "contact sheet composition was cancelled",
            details={"sheet_id": record.sheet_id, "state": record.state},
        )

    def _raise_if_cancelled(
        self,
        record: ContactSheetRecord,
        *,
        original: ApplicationError | None = None,
    ) -> None:
        if record.state == "cancelled":
            raise self._cancelled_response(record)
        if record.state == "failed" and original is not None:
            raise original

    def get(self, sheet_id: str) -> ContactSheetRecord:
        normalized = validate_identifier(sheet_id, "sheet_id")
        return self._repository.get_contact_sheet(normalized)

    def get_artifact(self, sheet_id: str) -> bytes:
        normalized = validate_identifier(sheet_id, "sheet_id")
        record = self._repository.get_contact_sheet(normalized)
        if record.state != "ready":
            raise StateError(
                "contact sheet is not ready",
                details={"state": record.state},
            )
        return self._repository.get_contact_sheet_artifact(
            record.sheet_id,
            record.artifact_name,
        )
