"""Cancellation smoke paths for shot inspections and contact sheets.

The checks exercise four guarantees:

1. an in-process cancellation stops frame decoding/compositing and settles a
   durable ``cancelled`` record that never becomes a success;
2. a cancellation issued by a second service process on the shared data root
   is honored by the worker process;
3. when cancellation and completion race, the first terminal state wins and
   every later read observes the same final outcome;
4. a late cancellation can never rewrite an already completed artifact.
"""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path

from frame_proxy.media.avi_writer import build_sample_avi
from tools.harness import (
    PauseGate,
    RunningService,
    HttpResult,
    require,
    require_status,
    success,
)

LONG_SCENES = tuple([8] * 12)
INSPECTION_BODY = {
    "sensitivity": 0.55,
    "sample_stride": 1,
    "min_scene_frames": 2,
}
SHEET_BODY = {
    "columns": 3,
    "tile_width": 96,
    "gutter": 4,
    "selection": "stored",
    "background": "#20252c",
}


def _post(
    service: RunningService,
    path: str,
    body: dict,
) -> HttpResult:
    return service.request_json("POST", path, body)


def _upload_clip(service: RunningService, clip_id: str) -> None:
    payload = build_sample_avi(scene_lengths=LONG_SCENES)
    require_status(
        service.request(
            "PUT",
            f"/v1/clips/{clip_id}",
            payload,
            "application/x-avi",
        ),
        201,
        "clip upload",
    )


def check_inspection_cancelled_in_process() -> None:
    gate = PauseGate()
    with RunningService(gate=gate) as service:
        _upload_clip(service, "clip-cancel-ip")
        gate.pause_at("inspection", "insp-cancel-ip", progress=3)
        worker = {}
        thread = threading.Thread(
            target=lambda: worker.update(
                result=_post(
                    service,
                    "/v1/clips/clip-cancel-ip/inspections",
                    {**INSPECTION_BODY, "inspection_id": "insp-cancel-ip"},
                )
            )
        )
        thread.start()
        gate.wait_until_reached("inspection", "insp-cancel-ip")
        reached = gate.reached_progress("inspection", "insp-cancel-ip")
        cancel = _post(
            service,
            "/v1/inspections/insp-cancel-ip/cancellation",
            {},
        )
        require_status(cancel, 200, "inspection cancellation")
        require(
            cancel.json()["cancellation"]["outcome"] == "cancelled",
            "cancellation outcome must be cancelled",
        )
        gate.release("inspection", "insp-cancel-ip")
        thread.join(timeout=10)
        require(not thread.is_alive(), "inspection worker did not stop")

        result = worker["result"]
        require_status(result, 409, "cancelled create response")
        require(
            result.json()["error"]["code"] == "operation_cancelled",
            "cancelled create must carry operation_cancelled",
        )

        read = service.request("GET", "/v1/inspections/insp-cancel-ip")
        record = read.json()["inspection"]
        require(
            record["state"] == "cancelled",
            "inspection must settle as cancelled",
        )
        require(
            record["error_code"] == "operation_cancelled",
            "cancelled record needs a stable error code",
        )
        require(
            record["scenes"] == [],
            "cancelled inspection must not expose scene data",
        )
        # The worker paused at frame 3; no frame after that may be decoded.
        require(
            gate.reached_progress("inspection", "insp-cancel-ip") == reached,
            "worker kept decoding frames after cancellation",
        )
        terminal_states = [
            entry["to"] for entry in record["transitions"] if entry["to"] == "indexed"
        ]
        require(terminal_states == [], "cancelled job must never pass through indexed")

        # A sheet can never be built from a cancelled inspection.
        sheet = _post(
            service,
            "/v1/inspections/insp-cancel-ip/contact-sheets",
            {**SHEET_BODY, "sheet_id": "sheet-from-cancelled"},
        )
        require_status(sheet, 409, "sheet from cancelled inspection")

        # Cancellation is idempotent and still reads cancelled afterwards.
        again = _post(
            service,
            "/v1/inspections/insp-cancel-ip/cancellation",
            {},
        )
        require_status(again, 200, "idempotent cancellation")


def check_contact_sheet_cancelled_in_process() -> None:
    gate = PauseGate()
    with RunningService(gate=gate) as service:
        _upload_clip(service, "clip-sheet-cancel")
        indexed = _post(
            service,
            "/v1/clips/clip-sheet-cancel/inspections",
            {**INSPECTION_BODY, "inspection_id": "insp-sheet-cancel"},
        )
        require_status(indexed, 201, "inspection setup")

        gate.pause_at("contact-sheet", "sheet-cancel-ip", progress=2)
        worker = {}
        thread = threading.Thread(
            target=lambda: worker.update(
                result=_post(
                    service,
                    "/v1/inspections/insp-sheet-cancel/contact-sheets",
                    {**SHEET_BODY, "sheet_id": "sheet-cancel-ip"},
                )
            )
        )
        thread.start()
        gate.wait_until_reached("contact-sheet", "sheet-cancel-ip")
        reached = gate.reached_progress("contact-sheet", "sheet-cancel-ip")
        cancel = _post(
            service,
            "/v1/contact-sheets/sheet-cancel-ip/cancellation",
            {},
        )
        require_status(cancel, 200, "sheet cancellation")
        gate.release("contact-sheet", "sheet-cancel-ip")
        thread.join(timeout=10)
        require(not thread.is_alive(), "sheet worker did not stop")

        require_status(worker["result"], 409, "cancelled sheet create response")
        read = service.request("GET", "/v1/contact-sheets/sheet-cancel-ip")
        record = read.json()["contact_sheet"]
        require(record["state"] == "cancelled", "sheet must settle as cancelled")
        require(
            record["artifact_digest"] is None,
            "cancelled sheet must not carry an artifact digest",
        )
        require(
            gate.reached_progress("contact-sheet", "sheet-cancel-ip") == reached,
            "worker kept compositing after cancellation",
        )
        artifact = service.request(
            "GET",
            "/v1/contact-sheets/sheet-cancel-ip/artifact",
        )
        require_status(artifact, 409, "cancelled sheet artifact must stay hidden")


def check_cross_process_cancellation() -> None:
    with tempfile.TemporaryDirectory(prefix="frame-proxy-shared-") as directory:
        shared_root = Path(directory)
        worker_gate = PauseGate()
        with RunningService(data_root=shared_root, gate=worker_gate) as worker_service:
            _upload_clip(worker_service, "clip-xproc")
            worker_gate.pause_at("inspection", "insp-xproc", progress=4)
            worker = {}
            thread = threading.Thread(
                target=lambda: worker.update(
                    result=_post(
                        worker_service,
                        "/v1/clips/clip-xproc/inspections",
                        {**INSPECTION_BODY, "inspection_id": "insp-xproc"},
                    )
                )
            )
            thread.start()
            worker_gate.wait_until_reached("inspection", "insp-xproc")

            # A separate process/instance rooted at the same data directory.
            with RunningService(data_root=shared_root) as other_service:
                cancel = _post(
                    other_service,
                    "/v1/inspections/insp-xproc/cancellation",
                    {},
                )
                require_status(cancel, 200, "cross-process cancellation")
                require(
                    cancel.json()["inspection"]["state"] == "cancelled",
                    "other process must read cancelled",
                )

            worker_gate.release("inspection", "insp-xproc")
            thread.join(timeout=10)
            require(not thread.is_alive(), "cross-process worker did not stop")
            require_status(worker["result"], 409, "cross-process create response")

            with RunningService(data_root=shared_root) as reader:
                read = reader.request("GET", "/v1/inspections/insp-xproc")
                require(
                    read.json()["inspection"]["state"] == "cancelled",
                    "a third process must still read cancelled",
                )


def check_completion_cancel_race() -> None:
    gate = PauseGate()
    with RunningService(gate=gate) as service:
        _upload_clip(service, "clip-race")

        # Branch A: cancel lands while the worker is paused at its publication
        # checkpoint -> cancellation must win and indexed must never appear.
        gate.pause_at("inspection", "insp-race-a", progress=3)
        worker_a = {}
        thread_a = threading.Thread(
            target=lambda: worker_a.update(
                result=_post(
                    service,
                    "/v1/clips/clip-race/inspections",
                    {**INSPECTION_BODY, "inspection_id": "insp-race-a"},
                )
            )
        )
        thread_a.start()
        gate.wait_until_reached("inspection", "insp-race-a")
        cancel_a = _post(
            service,
            "/v1/inspections/insp-race-a/cancellation",
            {},
        )
        require_status(cancel_a, 200, "race A cancellation")
        gate.release("inspection", "insp-race-a")
        thread_a.join(timeout=10)
        final_a = service.request("GET", "/v1/inspections/insp-race-a")
        require(
            final_a.json()["inspection"]["state"] == "cancelled",
            "race A final state must be cancelled",
        )
        # Repeated reads must keep returning the same terminal outcome.
        final_a_again = service.request("GET", "/v1/inspections/insp-race-a")
        require(
            final_a_again.json()["inspection"]["transitions"]
            == final_a.json()["inspection"]["transitions"],
            "terminal record must be stable across reads",
        )

        # Branch B: the job finishes first; a late cancel must be superseded and
        # the completed result must remain the single visible outcome.
        indexed = _post(
            service,
            "/v1/clips/clip-race/inspections",
            {**INSPECTION_BODY, "inspection_id": "insp-race-b"},
        )
        require_status(indexed, 201, "race B inspection completion")
        late_cancel = _post(
            service,
            "/v1/inspections/insp-race-b/cancellation",
            {},
        )
        require_status(late_cancel, 409, "late cancellation conflict")
        details = late_cancel.json()["error"]["details"]
        require(
            details["outcome"] == "superseded" and details["state"] == "indexed",
            "late cancel must name the winning state",
        )
        read_b = service.request("GET", "/v1/inspections/insp-race-b")
        require(
            read_b.json()["inspection"]["state"] == "indexed",
            "completed inspection must not be rewritten by a late cancel",
        )

        # The same rule for contact sheets and their artifact.
        sheet = _post(
            service,
            "/v1/inspections/insp-race-b/contact-sheets",
            {**SHEET_BODY, "sheet_id": "sheet-race-b"},
        )
        require_status(sheet, 201, "race B sheet completion")
        late_sheet_cancel = _post(
            service,
            "/v1/contact-sheets/sheet-race-b/cancellation",
            {},
        )
        require_status(late_sheet_cancel, 409, "late sheet cancellation conflict")
        artifact = service.request(
            "GET",
            "/v1/contact-sheets/sheet-race-b/artifact",
        )
        require_status(artifact, 200, "completed artifact must remain downloadable")
        record = service.request("GET", "/v1/contact-sheets/sheet-race-b")
        require(
            record.json()["contact_sheet"]["state"] == "ready",
            "ready sheet must not be rewritten by a late cancel",
        )


def check_unknown_job_cancellation() -> None:
    with RunningService() as service:
        missing = _post(
            service,
            "/v1/inspections/inspection-missing/cancellation",
            {},
        )
        require_status(missing, 404, "unknown inspection cancellation")
        missing_sheet = _post(
            service,
            "/v1/contact-sheets/sheet-missing/cancellation",
            {},
        )
        require_status(missing_sheet, 404, "unknown sheet cancellation")


def main() -> int:
    check_inspection_cancelled_in_process()
    success("cancel-inspection-in-process", {"settled": "cancelled"})
    check_contact_sheet_cancelled_in_process()
    success("cancel-contact-sheet-in-process", {"settled": "cancelled"})
    check_cross_process_cancellation()
    success("cancel-cross-process", {"shared_root": True})
    check_completion_cancel_race()
    success("cancel-completion-race", {"first_terminal_wins": True})
    check_unknown_job_cancellation()
    success("cancel-unknown-job", {"status": 404})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
