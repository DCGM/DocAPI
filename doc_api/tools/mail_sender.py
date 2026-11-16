import logging
import math
import smtplib
import ssl
from collections import deque
from dataclasses import dataclass
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from enum import Enum
from threading import Lock
from time import time
from typing import Mapping, Optional

logger = logging.getLogger(__name__)

SECURITY_MODES = ('auto', 'none', 'ssl', 'starttls')

X_MAILER = 'DocAPI MailSender'

NOT_SENT_NOTICE = 'An email with the subject {subject!r} could not be sent to you: {reason}.'


class MailStatus(Enum):
    """What happened to an email."""
    SENT = 'sent'            # The SMTP server accepted it.
    HELD_BACK = 'held_back'  # Not sent: the sender's limits hold it back until retry_at.
    FAILED = 'failed'        # Not sent: no valid receiver, or the SMTP send failed.


@dataclass(frozen=True)
class MailResult:
    """The outcome of MailSender.send; true only when the email was sent.

    retry_at: when HELD_BACK, the Unix time the limit that held it back has
    room again; the other limit may still hold it back then.
    reason: when HELD_BACK or FAILED, why, fit for a log.
    """
    status: MailStatus
    receiver: str
    retry_at: Optional[float] = None
    reason: Optional[str] = None

    def __bool__(self) -> bool:
        return self.status is MailStatus.SENT


@dataclass(frozen=True)
class _Admission:
    """What the limits decide for one email."""
    held_back: Optional[tuple[str, float]] = None  # (reason, retry_at) when it may not be sent.
    notice: Optional[str] = None                   # Why, when the receiver gets a NOT SENT notice.


class MailSender:
    """Sends emails through one SMTP server, each to exactly one receiver.

    sender = MailSender(server='smtp.mail.server.cz', sender_mail='worker@mail.server.cz')
    result = sender.send('text of subject', 'mail message', 'user@mail.server.cz')
    if result.status is MailStatus.HELD_BACK: ...

    To email several people, call send once for each: then the limits count
    what the SMTP server delivers, and each receiver has its own result.

    security: 'ssl' connects over SSL, 'starttls' upgrades a plain connection,
    'none' sends in plain text and 'auto' is 'ssl' with a password, 'none'
    without. The server is logged in to as username (sender_mail by default)
    whenever a password is given. Over TLS the server's certificate must be
    valid for server and signed by a CA the system trusts, or by ca_file (a PEM
    file of trusted CA certificates); otherwise the send fails.

    Keep one sender for as long as its limits should hold; both count send
    attempts, failed ones included, and 0 turns a limit off. The receiver of an
    email a limit holds back gets a "<subject> - NOT SENT" notice instead, at
    most once per limit and window; it does not say whether the email is sent
    later, which is up to the caller:

    send_receiver_limit: at most this many emails with one subject to one
    receiver per send_receiver_window_seconds (a sliding window). It protects
    the receivers.
    send_server_limit: at most this many emails through this sender, notices
    included, per send_server_window_seconds (a sliding window). It protects
    the SMTP server. About a tenth of it (send_server_notice_reserve) is kept
    for notices; regular email may use send_server_regular_limit.
    """

    def __init__(self,
                 server: str,
                 sender_mail: str,
                 port: int = 25,
                 sender_name: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 security: str = 'auto',
                 ca_file: Optional[str] = None,
                 headers: Optional[Mapping[str, str]] = None,
                 charset: str = 'utf-8',
                 content_type: str = 'text/plain',
                 timeout: float = 60,
                 send_receiver_limit: int = 10,
                 send_receiver_window_seconds: float = 3600,
                 send_server_limit: int = 100,
                 send_server_window_seconds: float = 3600):
        if not server:
            raise ValueError('An SMTP server is required')
        if not sender_mail:
            raise ValueError('A sender mail is required')
        if security not in SECURITY_MODES:
            raise ValueError(f'Unknown mail security {security!r}, expected one of {", ".join(SECURITY_MODES)}')
        if send_receiver_limit < 0 or send_server_limit < 0:
            raise ValueError('Mail send receiver and server limits must not be negative')
        for window in (send_receiver_window_seconds, send_server_window_seconds):
            if not math.isfinite(window) or window <= 0:
                raise ValueError('Mail send windows must be a finite, positive number of seconds')
        if security == 'auto':
            security = 'ssl' if password is not None else 'none'
        # Built now so a bad ca_file fails here, not on the first send.
        self.ca_file = ca_file
        self._tls_context = ssl.create_default_context(cafile=ca_file)

        self.server = server
        self.port = int(port)
        self.sender_mail = sender_mail
        self.sender_name = sender_name
        self.username = username or sender_mail
        self.password = password
        self.security = security
        self.headers = dict(headers or {})
        self.charset = charset
        self.content_type = content_type
        self.timeout = timeout
        self.send_receiver_limit = int(send_receiver_limit)
        self.send_receiver_window_seconds = float(send_receiver_window_seconds)
        self.send_server_limit = int(send_server_limit)
        self.send_server_window_seconds = float(send_server_window_seconds)
        # The send server limit splits into the part regular email may use and a
        # reserve on top of it that only NOT SENT notices may use.
        self.send_server_notice_reserve = (min(self.send_server_limit - 1, math.ceil(self.send_server_limit / 10))
                                           if self.send_server_limit else 0)
        self.send_server_regular_limit = self.send_server_limit - self.send_server_notice_reserve

        self._limits_lock = Lock()  # Guards all the limit state below.
        # Send receiver limit, per (receiver, subject): when each email in the
        # window was sent, oldest first, and when the last notice was sent.
        self._receiver_send_times_in_window: dict[tuple[str, str], deque[float]] = {}
        self._receiver_limit_notice_time: dict[tuple[str, str], float] = {}
        # Send server limit: when each email in the window was sent, oldest
        # first, and when each receiver last got a send server limit notice.
        self._server_send_times_in_window: deque[float] = deque()
        self._server_limit_notice_time_by_receiver: dict[str, float] = {}

    @property
    def fromaddr(self) -> str:
        if self.sender_name is not None:
            return formataddr((self.sender_name, self.sender_mail))
        return self.sender_mail

    def send(self,
             subject: str,
             body: str,
             receiver: str,
             headers: Optional[Mapping[str, str]] = None) -> MailResult:
        """Email body now to receiver, one address.

        Never raises: the result says whether the email was sent, held back by
        the limits or failed, and each outcome is logged with the sender and the
        receiver, never the body.
        """
        if not isinstance(receiver, str) or not receiver.strip() or ',' in receiver:
            logger.error(f'Failed to send email {subject!r} from {self.fromaddr} to {receiver!r}: '
                         f'not one address')
            return MailResult(MailStatus.FAILED, str(receiver), reason='the receiver is not one address')
        admission = self._admit(subject, receiver)
        if admission.notice is not None:
            try:
                self._deliver(f'{subject} - NOT SENT',
                              NOT_SENT_NOTICE.format(subject=subject, reason=admission.notice), receiver, headers)
            except Exception:
                logger.exception(f'Failed to send the NOT SENT notice for {subject!r} to {receiver}')
        if admission.held_back is not None:
            reason, retry_at = admission.held_back
            logger.warning(f'Email {subject!r} from {self.fromaddr} to {receiver} held back by the {reason}')
            return MailResult(MailStatus.HELD_BACK, receiver, retry_at=retry_at, reason=reason)
        try:
            self._deliver(subject, body, receiver, headers)
        except Exception as error:
            logger.exception(f'Failed to send email {subject!r} from {self.fromaddr} to {receiver}')
            return MailResult(MailStatus.FAILED, receiver, reason=f'{type(error).__name__}: {error}')
        logger.info(f'Sent email {subject!r} from {self.fromaddr} to {receiver}')
        return MailResult(MailStatus.SENT, receiver)

    def _admit(self, subject: str, receiver: str) -> _Admission:
        """Count one email under both limits, or hold it back with a notice."""
        now = time()
        with self._limits_lock:
            self._drop_sends_outside_windows(now)
            if self.send_server_limit and len(self._server_send_times_in_window) >= self.send_server_regular_limit:
                return self._admit_over_server_limit(receiver.casefold(), now)
            return self._admit_under_receiver_limit((receiver.casefold(), subject), now)

    def _drop_sends_outside_windows(self, now: float):
        server_window_start = now - self.send_server_window_seconds
        while self._server_send_times_in_window and self._server_send_times_in_window[0] <= server_window_start:
            self._server_send_times_in_window.popleft()
        self._server_limit_notice_time_by_receiver = {
            receiver: noticed_at for receiver, noticed_at in self._server_limit_notice_time_by_receiver.items()
            if noticed_at > server_window_start}

        receiver_window_start = now - self.send_receiver_window_seconds
        for key, send_times in list(self._receiver_send_times_in_window.items()):
            while send_times and send_times[0] <= receiver_window_start:
                send_times.popleft()
            if not send_times:
                del self._receiver_send_times_in_window[key]
        self._receiver_limit_notice_time = {
            key: noticed_at for key, noticed_at in self._receiver_limit_notice_time.items()
            if noticed_at > receiver_window_start}

    def _admit_over_server_limit(self, receiver: str, now: float) -> _Admission:
        """Hold the email back; notify the receiver once per window."""
        reason = f'send server limit of {self.send_server_limit} emails per {self.send_server_window_seconds:g} s'
        notice = None
        if receiver not in self._server_limit_notice_time_by_receiver and self._notice_fits_server_limit():
            self._server_limit_notice_time_by_receiver[receiver] = now
            self._count_server_send(now)
            notice = f'the sender reached its {reason}'
        # Regular email may go again once enough sends leave the window.
        first_send_to_leave = len(self._server_send_times_in_window) - self.send_server_regular_limit
        retry_at = self._server_send_times_in_window[first_send_to_leave] + self.send_server_window_seconds
        return _Admission((reason, retry_at), notice)

    def _admit_under_receiver_limit(self, key: tuple[str, str], now: float) -> _Admission:
        """Send under the send receiver limit; otherwise hold the email back
        and notify the receiver once per window."""
        if not self.send_receiver_limit:
            self._count_server_send(now)
            return _Admission()
        send_times = self._receiver_send_times_in_window.setdefault(key, deque())
        if len(send_times) < self.send_receiver_limit:
            send_times.append(now)
            self._count_server_send(now)
            return _Admission()
        window = f'{self.send_receiver_window_seconds:g} s'
        reason = f'send receiver limit of {self.send_receiver_limit} emails per receiver and subject per {window}'
        notice = None
        if key not in self._receiver_limit_notice_time and self._notice_fits_server_limit():
            self._receiver_limit_notice_time[key] = now
            self._count_server_send(now)
            notice = f'{self.send_receiver_limit} emails with this subject were already sent to you within {window}'
        # The receiver may get the subject again once enough sends leave the window.
        first_send_to_leave = len(send_times) - self.send_receiver_limit
        retry_at = send_times[first_send_to_leave] + self.send_receiver_window_seconds
        return _Admission((reason, retry_at), notice)

    def _notice_fits_server_limit(self) -> bool:
        return not self.send_server_limit or len(self._server_send_times_in_window) < self.send_server_limit

    def _count_server_send(self, now: float):
        if self.send_server_limit:
            self._server_send_times_in_window.append(now)

    def _deliver(self, subject: str, body: str, receiver: str, headers: Optional[Mapping[str, str]]):
        """One SMTP transaction to one receiver; raises on failure, also when
        the server refuses the receiver."""
        subtype = self.content_type.split('/')[-1]
        email = MIMEText(body, subtype, 'us-ascii' if body.isascii() else self.charset)
        for header, value in {**self.headers, **(headers or {})}.items():
            email[header] = value
        email['Subject'] = subject
        email['From'] = self.fromaddr
        email['To'] = receiver
        email['X-Mailer'] = X_MAILER
        email['Date'] = formatdate()
        email['Message-ID'] = make_msgid()

        if self.security == 'ssl':
            smtp = smtplib.SMTP_SSL(self.server, self.port, timeout=self.timeout, context=self._tls_context)
        else:
            smtp = smtplib.SMTP(self.server, self.port, timeout=self.timeout)
        with smtp:
            if self.security == 'starttls':
                smtp.starttls(context=self._tls_context)
            if self.password is not None:
                smtp.login(self.username, self.password)
            smtp.sendmail(self.sender_mail, [receiver], email.as_string())
