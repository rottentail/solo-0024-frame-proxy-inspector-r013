from __future__ import annotations

import hashlib

from frame_proxy.config import Settings
from frame_proxy.domain.errors import (
    ApplicationError,
    CancellationError,
    ErrorCode,
    MediaValidationError,
)
from frame_proxy.domain.models import InspectionRecord
from frame_proxy.domain.policies import (
    InspectionRequest,
    validate_clip_id,
    validate_identifier,
)
from frame_proxy.domain.transitions import transition_inspection
from frame_proxy.media.avi import probe_avi
from frame_proxy.media.detector import analyze_frames
from frame_proxy.media.fingerprint import fingerprint_frame
from frame_proxy.media.frames import decode_frame_at
from frame_proxy.service.cancellation import (
    CancellationRegistry,
    WorkflowGate,
)
from frame_proxy.storage.repository import FileRepository

JOB_KIND = "inspection"


class InspectionService:
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
        clip_id: str,
        request: InspectionRequest,
    ) -> InspectionRecord:
        normalized_clip_id = validate_clip_id(clip_id)
        clip = self._repository.get_clip(normalized_clip_id)
        if clip.state != "registered":
            raise MediaValidationError(
                "clip is not ready for inspection",
                code=ErrorCode.INVALID_STATE,
            )

        record = InspectionRecord(
            inspection_id=request.inspection_id,
            clip_id=clip.clip_id,
            state="pending",
            sensitivity=request.sensitivity,
            sample_stride=request.sample_stride,
            min_scene_frames=request.min_scene_frames,
        )
        self._repository.create_inspection(record)

        token = self._registry.token(
            JOB_KIND,
            record.inspection_id,
            durable_checkpoint=lambda: self._repository.cancellation_marker_exists(
                JOB_KIND,
                record.inspection_id,
            ),
        )
        token.checkpoint(stage="creation")
        transition_inspection(record, "scanning", reason="frame scan started")
        self._repository.update_inspection(record)

        try:
            media = self._repository.get_clip_media(clip.clip_id)
            if hashlib.sha256(media).hexdigest() != clip.digest:
                raise MediaValidationError(
                    "stored clip digest does not match its record",
                    code=ErrorCode.FRAME_DECODE,
                )
            descriptor = probe_avi(media)
            sampled_frames = tuple(
                range(0, descriptor.frame_count, request.sample_stride)
            )
            fingerprints = []
            for progress, frame_index in enumerate(sampled_frames):
                token.checkpoint(stage="frame decode")
                self._gate.await_gate(
                    JOB_KIND,
                    record.inspection_id,
                    progress=progress,
                )
                fingerprints.append(
                    fingerprint_frame(decode_frame_at(media, descriptor, frame_index))
                )
            token.checkpoint(stage="cut detection")
            result = analyze_frames(
                descriptor.frame_count,
                sampled_frames,
                tuple(fingerprints),
                sensitivity=request.sensitivity,
                min_scene_frames=request.min_scene_frames,
            )
            record.sampled_frames = list(result.sampled_frames)
            record.cut_frames = list(result.cut_frames)
            record.scenes = list(result.scenes)
            token.checkpoint(stage="publication")
            settled = self._settle(record, "indexed")
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
                "shot indexing failed",
                status=500,
            ) from exc
        finally:
            self._registry.discard(JOB_KIND, record.inspection_id)

    def cancel(self, inspection_id: str) -> InspectionRecord:
        normalized = validate_identifier(inspection_id, "inspection_id")
        record = self._repository.get_inspection(normalized)
        # Publish the durable decision before touching any state so workers in
        # other processes observe it at their next checkpoint.
        self._repository.write_cancellation_marker(
            JOB_KIND,
            normalized,
            reason="client requested cancellation",
        )
        self._registry.signal(JOB_KIND, normalized)
        record.error_code = ErrorCode.OPERATION_CANCELLED.value
        record.error_message = "shot indexing was cancelled before completion"
        settled = self._settle(record, "cancelled")
        return settled

    def _settle(
        self,
        record: InspectionRecord,
        terminal_state: str,
    ) -> InspectionRecord:
        try:
            return self._repository.settle_inspection(record, terminal_state)
        except ApplicationError:
            raise
        except Exception as exc:
            raise ApplicationError(
                ErrorCode.STORAGE,
                "inspection state could not be committed",
                status=500,
            ) from exc

    def _settle_cancelled(self, record: InspectionRecord) -> InspectionRecord:
        record.error_code = ErrorCode.OPERATION_CANCELLED.value
        record.error_message = "shot indexing was cancelled before completion"
        return self._settle(record, "cancelled")

    def _settle_failure(
        self,
        record: InspectionRecord,
        code: str,
        message: str,
    ) -> InspectionRecord:
        record.error_code = code
        record.error_message = message
        try:
            return self._settle(record, "failed")
        except ApplicationError:
            return self._repository.get_inspection(record.inspection_id)

    def _cancelled_response(self, record: InspectionRecord) -> CancellationError:
        return CancellationError(
            "shot indexing was cancelled",
            details={
                "inspection_id": record.inspection_id,
                "state": record.state,
            },
        )

    def _raise_if_cancelled(
        self,
        record: InspectionRecord,
        *,
        original: ApplicationError | None = None,
    ) -> None:
        if record.state == "cancelled":
            raise self._cancelled_response(record)
        if record.state == "failed" and original is not None:
            raise original

    def get(self, inspection_id: str) -> InspectionRecord:
        normalized = validate_identifier(inspection_id, "inspection_id")
        return self._repository.get_inspection(normalized)
