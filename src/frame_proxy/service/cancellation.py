"""Cancellation coordination for long-running workflows.

Two coordination channels exist side by side:

* :class:`CancellationRegistry` holds per-job in-process events so a worker
  thread observes a cancellation issued by another thread immediately.
* Durable cancellation markers live in the shared data root (see
  :class:`frame_proxy.storage.repository.FileRepository`) so a worker running
  in a different service process honors the same decision.

A :class:`CancelToken` is the only cancellation surface handed to worker
code. It is a value object: every worker owns its own token and never shares
mutable state with the registry directly.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass

from frame_proxy.domain.errors import CancellationError

# A checkpoint answers the single question "has this job been cancelled?"
# Storage-backed checks are implemented as zero-argument callables so the
# service layer stays free of concrete repository imports in worker loops.
Checkpoint = Callable[[], bool]


@dataclass(frozen=True, slots=True)
class CancelToken:
    """Cooperative cancellation handle passed to a single worker run."""

    job_kind: str
    job_id: str
    _is_cancelled: Checkpoint

    def cancelled(self) -> bool:
        try:
            return bool(self._is_cancelled())
        except Exception:
            # A coordination hiccup must never silently kill a worker; the
            # durable settlement step still guarantees a single final state.
            return False

    def checkpoint(self, *, stage: str) -> None:
        """Raise :class:`CancellationError` at a cooperative stop point."""

        if self.cancelled():
            raise CancellationError(
                f"{self.job_kind} {self.job_id} was cancelled during {stage}",
                details={"job_kind": self.job_kind, "job_id": self.job_id, "stage": stage},
            )


class CancellationRegistry:
    """Process-local table of cancellation events keyed by ``(kind, id)``."""

    def __init__(self) -> None:
        self._events: dict[tuple[str, str], threading.Event] = {}
        self._guard = threading.Lock()

    def _key(self, job_kind: str, job_id: str) -> tuple[str, str]:
        return (job_kind, job_id)

    def register(self, job_kind: str, job_id: str) -> threading.Event:
        with self._guard:
            event = self._events.get(self._key(job_kind, job_id))
            if event is None:
                event = threading.Event()
                self._events[self._key(job_kind, job_id)] = event
            return event

    def signal(self, job_kind: str, job_id: str) -> None:
        self.register(job_kind, job_id).set()

    def is_signalled(self, job_kind: str, job_id: str) -> bool:
        with self._guard:
            event = self._events.get(self._key(job_kind, job_id))
        return event is not None and event.is_set()

    def token(
        self,
        job_kind: str,
        job_id: str,
        *,
        durable_checkpoint: Checkpoint | None = None,
    ) -> CancelToken:
        event = self.register(job_kind, job_id)

        def _check() -> bool:
            if event.is_set():
                return True
            if durable_checkpoint is not None:
                return bool(durable_checkpoint())
            return False

        return CancelToken(job_kind, job_id, _check)

    def discard(self, job_kind: str, job_id: str) -> None:
        with self._guard:
            self._events.pop(self._key(job_kind, job_id), None)


class WorkflowGate:
    """Deterministic pause hook used by worker loops and cancellation tests.

    The production gate is a no-op. Tests substitute a gate that blocks a
    named worker at a known progress point so a cancellation can be issued
    while decoding or compositing is provably still in flight.
    """

    def await_gate(self, job_kind: str, job_id: str, *, progress: int) -> None:
        return None
