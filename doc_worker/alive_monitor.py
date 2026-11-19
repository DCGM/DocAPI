"""That the worker is alive: AliveMonitor mails ALIVE at startup and then at an
interval through the WorkerMail, whenever the worker has mail with receivers."""
import logging
import math
import threading
import time
from typing import Callable, Optional

from doc_worker.worker_mail import WorkerMail, duration, row

logger = logging.getLogger(__name__)


class AliveMonitor:
    """Mails ALIVE at startup and then every interval, on a thread of its own.

    A report gives the uptime, the time since the last pipeline returned, the
    active job and, with a job history, the job statistics. It shows that the
    process lives, not that processing progresses. Without mail or receivers it
    sends nothing and starts no thread. It does not replace DocAPI's job outcome
    notifications, and failures are mailed whether or not it is on.
    """

    # The thread that sends the periodic ALIVE mail.
    THREAD_NAME = "worker-alive"

    def __init__(self, mail: Optional[WorkerMail], mail_startup: bool = True,
                 mail_interval_seconds: float = 86400,
                 active_job: Callable[[], Optional[str]] = lambda: None,
                 job_history: Callable[[], Optional[object]] = lambda: None):
        """
        Args:
            mail: Sends the reports; None, or mail without receivers, sends none.
            mail_startup: Send a report when the worker starts.
            mail_interval_seconds: Seconds between the reports; 0 sends no periodic reports.
            active_job: The id of the job being processed, None while idle.
            job_history: The worker's job history, anything with statistics() such as
                its JobHistory; None leaves the statistics out.
        """
        if not math.isfinite(mail_interval_seconds) or mail_interval_seconds < 0:
            raise ValueError("mail_interval_seconds must be a finite, nonnegative number")
        self.mail = mail
        self.mail_startup = mail_startup
        self.mail_interval_seconds = mail_interval_seconds
        self.active_job = active_job
        self.job_history = job_history
        self.started = time.monotonic()
        self.last_pipeline_return = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    @property
    def on(self) -> bool:
        """Whether there is anyone to send the reports to."""
        return self.mail is not None and bool(self.mail.receivers)

    def start(self):
        if not self.on:
            return
        if self.mail_startup:
            self.send_alive()
        if self.mail_interval_seconds > 0:
            thread = threading.Thread(target=self._run, name=self.THREAD_NAME, daemon=True)
            thread.start()
            self._thread = thread

    def pipeline_returned(self):
        with self._lock:
            self.last_pipeline_return = time.monotonic()

    def send_alive(self):
        now = time.monotonic()
        with self._lock:
            last_return = self.last_pipeline_return
        active_job, job_history = self.active_job(), self.job_history()
        lines = [
            row("uptime", duration(now - self.started)),
            row("last job", "none yet" if last_return is None else f"{duration(now - last_return)} ago"),
            row("active job", active_job if active_job is not None else "none active"),
        ]
        if job_history is not None:
            try:
                stats = job_history.statistics()
                if stats.get("invalid_records", 0):
                    lines += ["", "history", row("unreadable", str(stats["invalid_records"]) + " records; inspect worker log", " ")]
                for name, label in (("last_24_hours", "last 24 hours"), ("last_7_days", "last 7 days"),
                                    ("total", "all time")):
                    window = stats[name]
                    counts = window["outcomes"]
                    lines += ["", label,
                              row("attempts", window["attempts"], " "),
                              row("unique jobs", window["jobs"], " ")]
                    for outcome, outcome_label in (("ok", "ok"), ("error", "error"), ("fail", "failed"),
                                                   ("interrupted", "interrupted"), ("crashed", "crashed")):
                        lines.append(row(outcome_label, counts[outcome], " "))
                    lines.append(row("report fail", window["report_failures"], " "))
                    lines.append(row("runtime", duration(window["duration_seconds"]), " "))
            except Exception:
                logger.exception("Could not read worker job history statistics")
                lines += ["", "history", row("statistics", "unavailable; inspect worker log", " ")]
        self.mail.send("ALIVE", "\n".join(lines) + "\n")

    def _run(self):
        while not self._stop.wait(self.mail_interval_seconds):
            self.send_alive()

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
