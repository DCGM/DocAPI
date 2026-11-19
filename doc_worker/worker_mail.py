"""The worker's own emails: WorkerMail wraps a MailSender and sends each email
with the worker's identity, host, time and status.

The worker mails failures itself; doc_worker/outage_monitor.py mails DocAPI
outages and doc_worker/alive_monitor.py the ALIVE reports, both through a
WorkerMail. Emails about jobs come from DocAPI, not from here.
"""
import logging
import socket
import textwrap
from datetime import datetime, timezone
from typing import Optional, Sequence

from doc_api.tools.mail_sender import MailSender

logger = logging.getLogger(__name__)

DEFAULT_MAIL_SUBJECT = "[{identity}] {status} ({hostname})"


def check_mail_subject(subject: str) -> None:
    """Reject a subject template that cannot be filled in, e.g. one with an unknown placeholder."""
    subject.format(identity="worker", hostname="host", status="ALIVE", time=datetime.now(timezone.utc))


class WorkerMail:
    """Sends the worker's emails through a MailSender, to each receiver separately.

    send() mails an event of the worker (FAILURE, an outage status or ALIVE) with
    the subject template and a header of the worker's identity, host, time and
    status. send_raw() sends a given subject and body. Mail is sent as written;
    pass a MailSender subclass whose send() filters subject and body to mask or
    redact it. Without receivers, the worker's events are not mailed.
    """

    def __init__(self, sender: MailSender, receivers: Sequence[str] = (), identity: str = "worker",
                 hostname: Optional[str] = None, subject: str = DEFAULT_MAIL_SUBJECT):
        """
        Args:
            sender: Delivers each email.
            receivers: Who the worker's emails go to; each gets a separate email.
            identity: The worker's name, in the subject and the "worker" line.
            hostname: The host in the subject and the "hostname" line; socket.gethostname() by default.
            subject: Python format string with identity, hostname, status (ALIVE, FAILURE,
                DISCONNECTED, STILL DISCONNECTED or RECONNECTED) and time (UTC datetime,
                supporting date format specifications) placeholders.
        """
        check_mail_subject(subject)
        self.sender = sender
        self.receivers = list(receivers)
        self.identity = identity
        self.hostname = socket.gethostname() if hostname is None else hostname
        self.subject = subject
        if not self.receivers:
            logger.warning("Worker mail is off: the worker has a mail sender but no mail receivers")

    def send(self, status: str, message: str) -> None:
        """Mail an event of the worker to every receiver.

        Never raises: an alert that cannot be sent is logged, so it can neither
        interrupt processing nor cause another alert.
        """
        if not self.receivers:
            return
        now = datetime.now(timezone.utc)
        try:
            subject = self.subject.format(identity=self.identity, hostname=self.hostname, status=status, time=now)
            fields = (("worker", self.identity), ("hostname", self.hostname),
                      ("time", now.strftime("%Y-%m-%d %H:%M:%S UTC")), ("status", status))
            header = "\n".join(row(label, value) for label, value in fields)
            self.send_raw(subject, f"{header}\n\n{message}\n")
        except Exception:
            logger.exception("Could not send worker mail")

    def send_raw(self, subject: str, body: str, receivers: Optional[Sequence[str]] = None) -> bool:
        """Send this subject and body to each receiver, the worker's receivers by default.

        Returns:
            True if every receiver was sent the email; False otherwise, also when
            there is no receiver.
        """
        receivers = self.receivers if receivers is None else list(receivers)
        if not receivers:
            logger.warning(f"Not sending email {subject!r}: the worker has no mail receivers")
            return False
        results = [self.sender.send(subject, body, receiver) for receiver in receivers]
        return all(results)


# --- Formatting, shared with the monitors ---

def row(label: str, value: str, indent: str = "") -> str:
    return textwrap.fill(str(value), width=60,
                         initial_indent=f"{indent}{label:<12}",
                         subsequent_indent=indent + " " * 12,
                         break_long_words=False, break_on_hyphens=False)


def utc_text(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M:%S UTC")


def duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours or days:
        parts.append(f"{hours}h")
    if minutes or hours or days:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)
