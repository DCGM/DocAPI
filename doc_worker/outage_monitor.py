"""Whether DocAPI can be reached: OutageMonitor logs DocAPI outages and mails
DISCONNECTED, STILL DISCONNECTED and RECONNECTED through the WorkerMail."""
import logging
import math
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional

from doc_worker.worker_mail import WorkerMail, duration, row, utc_text

logger = logging.getLogger(__name__)


@dataclass
class _ServerOutage:
    """A time when the worker's requests could not reach DocAPI."""
    started_at: datetime
    started_monotonic: float
    job_id: Optional[str]           # The job being processed when it started.
    failed_requests: int = 0
    last_request: str = ""
    last_error: str = ""
    mailed: bool = False            # DISCONNECTED was sent.
    last_mail_monotonic: float = 0.0
    ended_at: Optional[datetime] = None
    ended_monotonic: Optional[float] = None


class OutageMonitor:
    """Tracks whether DocAPI can be reached, and mails its outages.

    Each request to DocAPI ends in unreachable() or reached(). An outage while
    the worker is idle is mailed as DISCONNECTED once it has lasted the grace
    period; one during a job at once. A STILL DISCONNECTED reminder follows each
    interval and RECONNECTED when DocAPI answers again. Failed requests are
    logged, not mailed one by one. Without mail, outages are only logged.

    When a job's outcome could not be reported, the worker reports it once DocAPI
    is back. RECONNECTED waits for that while report_pending() is true, and then
    carries the note the worker passes to report_settled().
    """

    def __init__(self, mail: Optional[WorkerMail], grace_seconds: float = 60,
                 mail_interval_seconds: float = 3600,
                 active_job: Callable[[], Optional[str]] = lambda: None,
                 report_pending: Callable[[], bool] = lambda: False):
        """
        Args:
            mail: Sends the outage emails; None only logs outages.
            grace_seconds: How long DocAPI may be unreachable while the worker is
                idle before a DISCONNECTED email.
            mail_interval_seconds: How often a STILL DISCONNECTED reminder is sent while
                DocAPI stays unreachable.
            active_job: The id of the job being processed, None while idle.
            report_pending: Whether a job's outcome is waiting to be reported.
        """
        for name, value, minimum in (("grace_seconds", grace_seconds, 0),
                                     ("mail_interval_seconds", mail_interval_seconds, None)):
            if not math.isfinite(value) or value < 0 or (minimum is None and value == 0):
                raise ValueError(f"{name} must be a finite, {'positive' if minimum is None else 'nonnegative'} number")
        self.mail = mail
        self.grace_seconds = grace_seconds
        self.mail_interval_seconds = mail_interval_seconds
        self._active_job = active_job
        self._report_pending = report_pending
        self._lock = threading.Lock()
        self._outage: Optional[_ServerOutage] = None

    def unreachable(self, request: str, error: str) -> None:
        """A request could not reach DocAPI: start or continue the outage, and mail
        DISCONNECTED or a STILL DISCONNECTED reminder when due."""
        logger.warning("DocAPI unreachable: %s: %s", request, error)
        now, now_monotonic = datetime.now(timezone.utc), time.monotonic()
        job = self._active_job()
        with self._lock:
            outage = self._outage
            if outage is None or outage.ended_at is not None:
                outage = self._outage = _ServerOutage(now, now_monotonic, None if job is None else str(job))
            outage.failed_requests += 1
            outage.last_request, outage.last_error = request, error
            status = None
            if not outage.mailed:
                # A job cannot be recovered mid-processing, so it is mailed at once.
                if job is not None or now_monotonic - outage.started_monotonic >= self.grace_seconds:
                    status, outage.mailed = "DISCONNECTED", True
                    if outage.job_id is None and job is not None:
                        outage.job_id = str(job)
            elif now_monotonic - outage.last_mail_monotonic >= self.mail_interval_seconds:
                status = "STILL DISCONNECTED"
            if status is not None:
                outage.last_mail_monotonic = now_monotonic
                message = self._message(outage, status, now_monotonic)
        if status is not None:
            self._send(status, message)

    def reached(self) -> None:
        """A request reached DocAPI: end the outage. RECONNECTED is mailed now,
        or by report_settled() if a job's outcome is waiting to be reported."""
        with self._lock:
            outage = self._outage
            if outage is None or outage.ended_at is not None:
                return
            outage.ended_at, outage.ended_monotonic = datetime.now(timezone.utc), time.monotonic()
            waiting = self._report_pending()
            if not waiting:
                self._outage = None
        logger.info("DocAPI is reachable again after %s", duration(outage.ended_monotonic - outage.started_monotonic))
        if not waiting:
            self._mail_reconnected(outage)

    def report_settled(self, note: str) -> None:
        """The outcome that waited for DocAPI was reported, or will never be: mail
        RECONNECTED with this note on the job, if DocAPI is back."""
        with self._lock:
            outage = self._outage
            if outage is not None and outage.ended_at is not None:
                self._outage = None
            else:
                outage = None
        if outage is not None:
            self._mail_reconnected(outage, lost_job=note)

    def _mail_reconnected(self, outage: _ServerOutage, lost_job: Optional[str] = None) -> None:
        if outage.mailed or lost_job is not None:
            self._send("RECONNECTED", self._message(outage, "RECONNECTED", lost_job=lost_job))

    def _send(self, status: str, message: str) -> None:
        if self.mail is not None:
            self.mail.send(status, message)

    @staticmethod
    def _message(outage: _ServerOutage, status: str, now_monotonic: Optional[float] = None,
                 *, lost_job: Optional[str] = None) -> str:
        if status == "RECONNECTED":
            lines = ["DocAPI is reachable again.", "",
                     row("from", utc_text(outage.started_at)),
                     row("to", utc_text(outage.ended_at)),
                     row("lasted", duration(outage.ended_monotonic - outage.started_monotonic)),
                     row("failed", f"{outage.failed_requests} requests"),
                     row("last error", outage.last_error)]
            if lost_job is not None:
                lines.append(row("job", lost_job))
            return "\n".join(lines)
        lines = ["DocAPI cannot be reached." if status == "DISCONNECTED" else "DocAPI is still unreachable.", "",
                 row("since", utc_text(outage.started_at))]
        if status != "DISCONNECTED":
            lines += [row("for", duration(now_monotonic - outage.started_monotonic)),
                      row("failed", f"{outage.failed_requests} requests")]
        lines += [row("request", outage.last_request), row("error", outage.last_error)]
        if outage.job_id is not None:
            lines.append(row("job", f"{outage.job_id}: it cannot be finished; its outcome is reported "
                                     f"once DocAPI is reachable again"))
        return "\n".join(lines)
