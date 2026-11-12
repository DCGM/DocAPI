"""Public monitoring payloads; never include credentials or job definitions/logs."""
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel

from doc_api.api.schemas.base_objects import ProcessingState, KeyRole

MonitorPeriod = Literal['24h', '7d', '1m', 'all']
MonitorJobSort = Literal['priority', 'created_date', 'started_date', 'finished_date', 'last_change']
MonitorSortOrder = Literal['desc', 'asc']


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


class MonitorStats(BaseModel):
    period: MonitorPeriod
    done: MonitorTotals
    failed: MonitorTotals
    attempts: MonitorTotals


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
    owner_label: str
    worker_label: str | None
    engine_name: str | None
    engine_version: str | None


class MonitorWorker(BaseModel):
    id: UUID
    label: str
    enabled: bool
    last_used: datetime | None
    status: Literal['active', 'idle', 'off']
    processing_jobs: int


class MonitorPrincipal(BaseModel):
    id: UUID
    label: str
    role: KeyRole


class MonitorEngine(BaseModel):
    name: str
    version: str
