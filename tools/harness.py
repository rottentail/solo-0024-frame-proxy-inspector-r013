from __future__ import annotations

import http.client
import json
import tempfile
import threading
from pathlib import Path
from typing import Any

from frame_proxy.app import build_router
from frame_proxy.config import Settings
from frame_proxy.http.server import FrameProxyServer, build_server
from frame_proxy.media.avi_writer import build_sample_avi
from frame_proxy.service.cancellation import WorkflowGate


class PauseGate(WorkflowGate):
    """Deterministic gate that blocks a named job at a chosen progress point."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._paused: dict[tuple[str, str], int] = {}
        self._reached: dict[tuple[str, str], int] = {}
        self._released: set[tuple[str, str]] = set()

    def pause_at(self, job_kind: str, job_id: str, progress: int) -> None:
        with self._condition:
            self._paused[(job_kind, job_id)] = progress
            self._released.discard((job_kind, job_id))

    def release(self, job_kind: str, job_id: str) -> None:
        with self._condition:
            self._released.add((job_kind, job_id))
            self._paused.pop((job_kind, job_id), None)
            self._condition.notify_all()

    def wait_until_reached(
        self,
        job_kind: str,
        job_id: str,
        timeout: float = 5.0,
    ) -> None:
        key = (job_kind, job_id)
        with self._condition:
            if not self._condition.wait_for(
                lambda: key in self._reached,
                timeout=timeout,
            ):
                raise RuntimeError(f"gate for {key} never reached")

    def reached_progress(self, job_kind: str, job_id: str) -> int | None:
        with self._condition:
            return self._reached.get((job_kind, job_id))

    def await_gate(self, job_kind: str, job_id: str, *, progress: int) -> None:
        key = (job_kind, job_id)
        with self._condition:
            self._reached[key] = progress
            self._condition.notify_all()
            if not self._condition.wait_for(
                lambda: key not in self._paused or key in self._released,
                timeout=10.0,
            ):
                raise RuntimeError(f"gate for {key} timed out")


class HttpResult:
    def __init__(self, status: int, content_type: str, body: bytes) -> None:
        self.status = status
        self.content_type = content_type
        self.body = body

    def json(self) -> dict[str, Any]:
        try:
            value = json.loads(self.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("response was not valid JSON") from exc
        if not isinstance(value, dict):
            raise RuntimeError("response JSON was not an object")
        return value


class RunningService:
    def __init__(
        self,
        *,
        data_root: Path | None = None,
        gate: WorkflowGate | None = None,
    ) -> None:
        self._temporary: tempfile.TemporaryDirectory[str] | None = None
        self._external_root = data_root
        self._gate = gate
        self._server: FrameProxyServer | None = None
        self._thread: threading.Thread | None = None
        self._port = 0

    def __enter__(self) -> "RunningService":
        if self._external_root is None:
            self._temporary = tempfile.TemporaryDirectory(prefix="frame-proxy-check-")
            root = Path(self._temporary.name)
        else:
            root = self._external_root
        settings = Settings(data_root=root)
        router = build_router(settings, gate=self._gate)
        self._server = build_server(
            "127.0.0.1",
            0,
            router=router,
            settings=settings,
        )
        self._port = int(self._server.server_address[1])
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            kwargs={"poll_interval": 0.02},
            daemon=True,
        )
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if self._temporary is not None:
            self._temporary.cleanup()

    def request(
        self,
        method: str,
        path: str,
        body: bytes = b"",
        content_type: str = "application/json",
    ) -> HttpResult:
        connection = http.client.HTTPConnection("127.0.0.1", self._port, timeout=10)
        try:
            headers = {"Content-Type": content_type}
            connection.request(method, path, body=body, headers=headers)
            response = connection.getresponse()
            return HttpResult(
                response.status,
                response.getheader("Content-Type", ""),
                response.read(),
            )
        finally:
            connection.close()

    def request_json(
        self,
        method: str,
        path: str,
        value: dict[str, Any],
    ) -> HttpResult:
        return self.request(
            method,
            path,
            json.dumps(value, separators=(",", ":")).encode("utf-8"),
        )

    def upload_clip(self, clip_id: str, payload: bytes | None = None) -> HttpResult:
        return self.request(
            "PUT",
            f"/v1/clips/{clip_id}",
            payload if payload is not None else build_sample_avi(),
            "application/x-avi",
        )

    def prepare_inspection(
        self,
        *,
        clip_id: str,
        inspection_id: str,
    ) -> dict[str, Any]:
        clip_response = self.upload_clip(clip_id)
        require_status(clip_response, 201, "clip setup")
        response = self.request_json(
            "POST",
            f"/v1/clips/{clip_id}/inspections",
            {
                "inspection_id": inspection_id,
                "sensitivity": 0.55,
                "sample_stride": 1,
                "min_scene_frames": 2,
            },
        )
        require_status(response, 201, "inspection setup")
        return response.json()["inspection"]


def require_status(result: HttpResult, expected: int, label: str) -> None:
    if result.status != expected:
        body = result.body.decode("utf-8", errors="replace")
        raise RuntimeError(f"{label} returned {result.status}: {body}")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def success(label: str, details: dict[str, Any]) -> None:
    print(f"PASS {label}: {json.dumps(details, sort_keys=True)}")

