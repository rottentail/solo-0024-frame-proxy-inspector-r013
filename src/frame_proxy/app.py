from __future__ import annotations

from frame_proxy.config import Settings
from frame_proxy.http.router import RequestRouter
from frame_proxy.service.cancellation import CancellationRegistry, WorkflowGate
from frame_proxy.service.clips import ClipService
from frame_proxy.service.contact_sheets import ContactSheetService
from frame_proxy.service.inspections import InspectionService
from frame_proxy.storage.repository import FileRepository


def build_router(
    settings: Settings,
    *,
    registry: CancellationRegistry | None = None,
    gate: WorkflowGate | None = None,
) -> RequestRouter:
    settings.ensure_ready()
    repository = FileRepository(settings.data_root)
    cancellation_registry = registry or CancellationRegistry()
    workflow_gate = gate or WorkflowGate()
    return RequestRouter(
        settings=settings,
        clips=ClipService(repository, settings),
        inspections=InspectionService(
            repository,
            settings,
            registry=cancellation_registry,
            gate=workflow_gate,
        ),
        contact_sheets=ContactSheetService(
            repository,
            settings,
            registry=cancellation_registry,
            gate=workflow_gate,
        ),
    )
