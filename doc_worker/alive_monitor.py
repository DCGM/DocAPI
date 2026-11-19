"""That the worker is alive: AliveMonitor mails ALIVE at startup and then at an
interval through the WorkerMailSender, whenever the worker has mail with receivers."""
import logging
import math
import threading
import time
from typing import Callable, Optional

from doc_worker.worker_mail_sender import WorkerMailSender, duration, row

logger = logging.getLogger(__name__)


class AliveMonitor:
    """Mails ALIVE at startup and then every interval, on a thread of its own.

    A report gives the uptime, the time since the worker last polled for a
    job, the active job and, with a job history, the job statistics. It shows
    that the process lives, not that processing progresses. _message() holds
    every text of the report. Without mail or receivers it sends nothing and
    starts no thread. It does not replace DocAPI's job outcome notifications,
    and failures are mailed whether or not it is on.
    """

    # The thread that sends the periodic ALIVE mail.
    THREAD_NAME = "worker-alive"

    def __init__(self, worker_mail_sender: Optional[WorkerMailSender], mail_startup: bool = True,
                 mail_interval_seconds: float = 86400,
                 active_job: Callable[[], Optional[str]] = lambda: None,
                 job_history: Callable[[], Optional[object]] = lambda: None):
        """
        Args:
            worker_mail_sender: Sends the reports; None, or one without receivers,
                sends none.
            mail_startup: Send a report when the worker starts.
            mail_interval_seconds: Seconds between the reports; 0 sends no periodic reports.
            active_job: The id of the job being processed, None while idle.
            job_history: The worker's job history, anything with statistics() such as
                its JobHistory; None leaves the statistics out.
        """
        if not math.isfinite(mail_interval_seconds) or mail_interval_seconds < 0:
            raise ValueError("mail_interval_seconds must be a finite, nonnegative number")
        self.worker_mail_sender = worker_mail_sender
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
        return self.worker_mail_sender is not None and bool(self.worker_mail_sender.receivers)

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
        uptime = now - self.started
        since_last_poll = None if last_return is None else now - last_return
        active_job, job_history = self.active_job(), self.job_history()
        try:
            statistics = None if job_history is None else job_history.statistics()
            message = self._message(uptime, since_last_poll, active_job, statistics)
        except Exception:
            logger.exception("Could not read worker job history statistics")
            message = self._message(uptime, since_last_poll, active_job, None, statistics_unavailable=True)
        self.worker_mail_sender.send("ALIVE", message)

    @staticmethod
    def _message(uptime: float, since_last_poll: Optional[float], active_job: Optional[str],
                 statistics: Optional[dict], statistics_unavailable: bool = False) -> str:
        """The text of an ALIVE mail; every text of these mails is here."""
        lines = [
            row("uptime", duration(uptime)),
            row("last poll", "none yet" if since_last_poll is None else f"{duration(since_last_poll)} ago"),
            row("active job", "none" if active_job is None else active_job),
        ]
        if statistics_unavailable:
            lines += ["", "job history", row("statistics", "unavailable; see the worker log", " ")]
        elif statistics is not None:
            for name, label in (("last_24_hours", "last 24 hours"), ("last_7_days", "last 7 days"),
                                ("total", "all time")):
                window = statistics[name]
                outcomes = window["outcomes"]
                lines += ["", label,
                          row("attempts", window["attempts"], " "),
                          row("jobs", window["jobs"], " ")]
                for outcome, outcome_label in (("ok", "ok"), ("error", "error"), ("fail", "failed"),
                                               ("interrupted", "interrupted"), ("crashed", "crashed")):
                    lines.append(row(outcome_label, outcomes[outcome], " "))
                lines += [row("unreported", window["report_failures"], " "),
                          row("runtime", duration(window["duration_seconds"]), " ")]
            # Unreadable history lines are left out of every count; only all time
            # spans them all.
            if statistics.get("invalid_records", 0):
                lines.append(row("unreadable", statistics["invalid_records"], " "))
            new = statistics.get("new_invalid_records", 0)
            if new:
                lines += ["", "job history",
                          row("file", statistics.get("path", "unknown"), " "),
                          row("unreadable", f"{new} new {'line' if new == 1 else 'lines'}, "
                                            f"left out of the statistics", " ")]
                lines += [row(f"line {invalid['line']}", invalid["reason"], " ")
                          for invalid in statistics.get("new_invalid_lines", ())]
        return "\n".join(lines)

    def _run(self):
        while not self._stop.wait(self.mail_interval_seconds):
            self.send_alive()

    def close(self):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=1)
