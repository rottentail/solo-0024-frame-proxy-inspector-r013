from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from frame_proxy.domain.errors import (
    ConflictError,
    ErrorCode,
    NotFoundError,
)
from frame_proxy.domain.models import (
    ClipRecord,
    ContactSheetRecord,
    InspectionRecord,
    utc_now,
)
from frame_proxy.domain.transitions import (
    transition_contact_sheet,
    transition_inspection,
)
from frame_proxy.storage.atomic import atomic_write_bytes, atomic_write_json
from frame_proxy.storage.lock import FileKeyLock
from frame_proxy.storage.paths import DataPaths


INSPECTION_TERMINAL_STATES = frozenset({"indexed", "failed", "cancelled"})
CONTACT_SHEET_TERMINAL_STATES = frozenset({"ready", "failed", "cancelled"})


class FileRepository:
    def __init__(self, root: Path) -> None:
        self.paths = DataPaths(root.expanduser().resolve())
        self.paths.ensure_ready()

    def _read_json(self, path: Path) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            raise
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"cannot read record {path.name}: {exc}") from exc
        if not isinstance(value, dict):
            raise RuntimeError(f"record {path.name} is not an object")
        return value

    def create_clip(self, record: ClipRecord, media: bytes) -> None:
        record_path = self.paths.clip_record(record.clip_id)
        media_path = self.paths.clip_media(record.clip_id)
        clip_dir = self.paths.clip_dir(record.clip_id)
        with FileKeyLock(self.paths.lock_path("clip", record.clip_id)):
            if record_path.exists():
                raise ConflictError(
                    "clip identifier already exists",
                    details={"clip_id": record.clip_id},
                )
            committed_media = False
            try:
                atomic_write_bytes(media_path, media)
                committed_media = True
                atomic_write_json(record_path, record.as_dict())
            except Exception:
                if committed_media:
                    try:
                        media_path.unlink()
                    except FileNotFoundError:
                        pass
                try:
                    if clip_dir.is_dir() and not any(clip_dir.iterdir()):
                        clip_dir.rmdir()
                except OSError:
                    pass
                raise

    def get_clip(self, clip_id: str) -> ClipRecord:
        path = self.paths.clip_record(clip_id)
        try:
            return ClipRecord.from_dict(self._read_json(path))
        except FileNotFoundError:
            raise NotFoundError(
                ErrorCode.CLIP_NOT_FOUND,
                "clip was not found",
                details={"clip_id": clip_id},
            )

    def get_clip_media(self, clip_id: str) -> bytes:
        path = self.paths.clip_media(clip_id)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise NotFoundError(
                ErrorCode.CLIP_NOT_FOUND,
                "clip media was not found",
                details={"clip_id": clip_id},
            )

    def create_inspection(self, record: InspectionRecord) -> None:
        path = self.paths.inspection_record(record.inspection_id)
        with FileKeyLock(
            self.paths.lock_path("inspection", record.inspection_id)
        ):
            if path.exists():
                raise ConflictError(
                    "inspection identifier already exists",
                    details={"inspection_id": record.inspection_id},
                )
            atomic_write_json(path, record.as_dict())

    def update_inspection(self, record: InspectionRecord) -> None:
        path = self.paths.inspection_record(record.inspection_id)
        with FileKeyLock(
            self.paths.lock_path("inspection", record.inspection_id)
        ):
            if not path.exists():
                raise NotFoundError(
                    ErrorCode.INSPECTION_NOT_FOUND,
                    "inspection was not found",
                    details={"inspection_id": record.inspection_id},
                )
            atomic_write_json(path, record.as_dict())

    def settle_inspection(
        self,
        record: InspectionRecord,
        terminal_state: str,
    ) -> InspectionRecord:
        """Commit a terminal inspection state under the job's cross-process lock.

        The first terminal state wins: a completion that arrives after a
        cancellation (or the reverse) is rejected, and the durable stored
        record is returned so callers can always read which outcome was
        actually adopted.
        """

        path = self.paths.inspection_record(record.inspection_id)
        with FileKeyLock(
            self.paths.lock_path("inspection", record.inspection_id)
        ):
            if not path.exists():
                raise NotFoundError(
                    ErrorCode.INSPECTION_NOT_FOUND,
                    "inspection was not found",
                    details={"inspection_id": record.inspection_id},
                )
            stored = InspectionRecord.from_dict(self._read_json(path))
            if stored.state in INSPECTION_TERMINAL_STATES:
                return stored
            record.state = stored.state
            record.transitions = list(stored.transitions)
            record.updated_at = stored.updated_at
            transition_inspection(record, terminal_state, reason=terminal_state)
            atomic_write_json(path, record.as_dict())
            return InspectionRecord.from_dict(self._read_json(path))

    def get_inspection(self, inspection_id: str) -> InspectionRecord:
        path = self.paths.inspection_record(inspection_id)
        try:
            return InspectionRecord.from_dict(self._read_json(path))
        except FileNotFoundError:
            raise NotFoundError(
                ErrorCode.INSPECTION_NOT_FOUND,
                "inspection was not found",
                details={"inspection_id": inspection_id},
            )

    def create_contact_sheet(self, record: ContactSheetRecord) -> None:
        path = self.paths.contact_sheet_record(record.sheet_id)
        directory = self.paths.contact_sheet_dir(record.sheet_id)
        with FileKeyLock(
            self.paths.lock_path("contact-sheet", record.sheet_id)
        ):
            if path.exists():
                raise ConflictError(
                    "contact sheet identifier already exists",
                    details={"sheet_id": record.sheet_id},
                )
            atomic_write_json(path, record.as_dict())

    def update_contact_sheet(self, record: ContactSheetRecord) -> None:
        path = self.paths.contact_sheet_record(record.sheet_id)
        directory = self.paths.contact_sheet_dir(record.sheet_id)
        with FileKeyLock(
            self.paths.lock_path("contact-sheet", record.sheet_id)
        ):
            if not path.exists():
                raise NotFoundError(
                    ErrorCode.CONTACT_SHEET_NOT_FOUND,
                    "contact sheet was not found",
                    details={"sheet_id": record.sheet_id},
                )
            atomic_write_json(path, record.as_dict())

    def settle_contact_sheet(
        self,
        record: ContactSheetRecord,
        terminal_state: str,
        *,
        artifact: bytes | None = None,
    ) -> ContactSheetRecord:
        """Commit a terminal contact sheet state under the job's cross-process lock.

        ``ready`` publication writes the PPM artifact and its record as one
        locked step. The first terminal state wins: a late completion after a
        cancellation never publishes an artifact, and a late cancellation
        never removes an already ready artifact.
        """

        path = self.paths.contact_sheet_record(record.sheet_id)
        artifact_path = self.paths.contact_sheet_artifact(
            record.sheet_id,
            record.artifact_name,
        )
        with FileKeyLock(
            self.paths.lock_path("contact-sheet", record.sheet_id)
        ):
            if not path.exists():
                raise NotFoundError(
                    ErrorCode.CONTACT_SHEET_NOT_FOUND,
                    "contact sheet was not found",
                    details={"sheet_id": record.sheet_id},
                )
            stored = ContactSheetRecord.from_dict(self._read_json(path))
            if stored.state in CONTACT_SHEET_TERMINAL_STATES:
                return stored

            record.state = stored.state
            record.transitions = list(stored.transitions)
            record.updated_at = stored.updated_at
            transition_contact_sheet(
                record,
                terminal_state,
                reason=terminal_state,
            )

            artifact_visible = False
            try:
                if terminal_state == "ready":
                    if artifact is None:
                        raise ValueError(
                            "a ready contact sheet requires an artifact payload"
                        )
                    atomic_write_bytes(artifact_path, artifact)
                    artifact_visible = True
                atomic_write_json(path, record.as_dict())
            except Exception:
                if artifact_visible:
                    try:
                        artifact_path.unlink()
                    except FileNotFoundError:
                        pass
                raise
            return ContactSheetRecord.from_dict(self._read_json(path))

    def get_contact_sheet(self, sheet_id: str) -> ContactSheetRecord:
        path = self.paths.contact_sheet_record(sheet_id)
        try:
            return ContactSheetRecord.from_dict(self._read_json(path))
        except FileNotFoundError:
            raise NotFoundError(
                ErrorCode.CONTACT_SHEET_NOT_FOUND,
                "contact sheet was not found",
                details={"sheet_id": sheet_id},
            )

    def get_contact_sheet_artifact(
        self,
        sheet_id: str,
        name: str,
    ) -> bytes:
        path = self.paths.contact_sheet_artifact(sheet_id, name)
        try:
            return path.read_bytes()
        except FileNotFoundError:
            raise NotFoundError(
                ErrorCode.CONTACT_SHEET_NOT_FOUND,
                "contact sheet artifact was not found",
                details={"sheet_id": sheet_id},
            )

    def write_cancellation_marker(
        self,
        kind: str,
        identifier: str,
        *,
        reason: str,
    ) -> None:
        """Durably record a cancellation request visible to every process."""

        marker = {
            "job_kind": kind,
            "job_id": identifier,
            "reason": reason,
            "requested_at": utc_now(),
        }
        atomic_write_json(
            self.paths.cancellation_marker(kind, identifier),
            marker,
        )

    def cancellation_marker_exists(self, kind: str, identifier: str) -> bool:
        return self.paths.cancellation_marker(kind, identifier).exists()

