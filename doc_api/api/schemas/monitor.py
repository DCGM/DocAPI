"""Public monitoring payloads; never include credentials or job definitions/logs."""
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from doc_api.api.schemas.base_objects import ProcessingState



class MonitorInfo(BaseModel):
    server_name: str
    hostname: str
    ip_addresses: list[str]
    version: str
    commit: str | None
    started_at: datetime
    uptime_seconds: float
    server_time: datetime
    database_connected: bool
    refresh_interval_seconds: int
    worker_offline_after_seconds: int


class MonitorDatabase(BaseModel):
    database_name: str
    size_bytes: int
    connections: int
    active_connections: int
    idle_connections: int
    idle_in_transaction_connections: int
    lock_waiting_connections: int
    longest_active_query_seconds: float | None
    deadlocks: int
    stats_reset: datetime | None


class MonitorTotals(BaseModel):
    job_count: int
    image_count: int
    runtime_seconds: float | None


class MonitorStateStats(BaseModel):
    done: MonitorTotals
    failed: MonitorTotals
    attempts: MonitorTotals


class MonitorGroupStats(MonitorStateStats):
    """Totals cover all matching states, independently of the DONE/FAILED breakdown."""
    total: MonitorTotals


class MonitorKeyStats(MonitorGroupStats):
    id: UUID | None
    label: str | None


class MonitorEngineStats(MonitorGroupStats):
    name: str | None


class MonitorEngineVersionStats(MonitorEngineStats):
    version: str | None


class MonitorStats(MonitorStateStats):
    owners: list[MonitorKeyStats]
    workers: list[MonitorKeyStats]
    engines: list[MonitorEngineStats]
    engine_versions: list[MonitorEngineVersionStats]


class MonitorJob(BaseModel):
    id: UUID
    state: ProcessingState
    image_count: int
    progress: float
    previous_attempts: int | None
    created_date: datetime
    started_date: datetime | None
    finished_date: datetime | None
    last_change: datetime
    owner_label: str | None
    worker_label: str | None
    engine_name: str | None
    engine_version: str | None


class MonitorProcessingTotals(BaseModel):
    processing: int
    queued: int
    new: int


class MonitorProcessing(BaseModel):
    jobs: list[MonitorJob]
    totals: MonitorProcessingTotals
    image_totals: MonitorProcessingTotals


class MonitorWorker(BaseModel):
    id: UUID
    label: str
    last_used: datetime | None
    status: Literal['active', 'idle', 'off']
    processing_jobs: int
