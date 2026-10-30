import logging
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr, formatdate, make_msgid
from typing import List, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)

SECURITY_MODES = ('auto', 'none', 'ssl', 'starttls')

X_MAILER = 'DocAPI MailSender'


class MailSender:
    """Sends emails through one SMTP server.

    sender = MailSender(server='smtp.mail.server.cz', sender_mail='worker@mail.server.cz')
    sender.send('text of subject', 'mail message', ['user@mail.server.cz'])

    security: 'ssl' connects over SSL, 'starttls' upgrades a plain connection,
    'none' sends in plain text and 'auto' is 'ssl' with a password, 'none'
    without. The server is logged in to as username (sender_mail by default)
    whenever a password is given.
    """

    def __init__(self,
                 server: str,
                 sender_mail: str,
                 port: int = 25,
                 receiver_mails: Optional[Sequence[str]] = None,
                 sender_name: Optional[str] = None,
                 username: Optional[str] = None,
                 password: Optional[str] = None,
                 security: str = 'auto',
                 headers: Optional[Mapping[str, str]] = None,
                 charset: str = 'utf-8',
                 content_type: str = 'text/plain',
                 timeout: float = 60):
        if not server:
            raise ValueError('An SMTP server is required')
        if not sender_mail:
            raise ValueError('A sender mail is required')
        if security not in SECURITY_MODES:
            raise ValueError(f'Unknown mail security {security!r}, expected one of {", ".join(SECURITY_MODES)}')
        if security == 'auto':
            security = 'ssl' if password is not None else 'none'

        self.server = server
        self.port = int(port)
        self.sender_mail = sender_mail
        self.sender_name = sender_name
        self.receiver_mails: List[str] = list(receiver_mails or [])
        self.username = username or sender_mail
        self.password = password
        self.security = security
        self.headers = dict(headers or {})
        self.charset = charset
        self.content_type = content_type
        self.timeout = timeout

    @property
    def fromaddr(self) -> str:
        if self.sender_name is not None:
            return formataddr((self.sender_name, self.sender_mail))
        return self.sender_mail

    def send_message(self,
                     subject: str,
                     body: str,
                     receiver_mails: Optional[Sequence[str]] = None,
                     headers: Optional[Mapping[str, str]] = None) -> List[str]:
        """Send one email now and return its receivers, raising on failure.

        receiver_mails default to the sender's own receivers.
        """
        receivers = self.receiver_mails if receiver_mails is None else list(receiver_mails)
        if not receivers:
            raise ValueError('No receiver mails to send the email to')

        subtype = self.content_type.split('/')[-1]
        email = MIMEText(body, subtype, 'us-ascii' if body.isascii() else self.charset)
        for header, value in {**self.headers, **(headers or {})}.items():
            email[header] = value
        email['Subject'] = subject
        email['From'] = self.fromaddr
        email['To'] = ', '.join(receivers)
        email['X-Mailer'] = X_MAILER
        email['Date'] = formatdate()
        email['Message-ID'] = make_msgid()

        if self.security == 'ssl':
            smtp = smtplib.SMTP_SSL(self.server, self.port, timeout=self.timeout)
        else:
            smtp = smtplib.SMTP(self.server, self.port, timeout=self.timeout)
        with smtp:
            if self.security == 'starttls':
                smtp.starttls()
            if self.password is not None:
                smtp.login(self.username, self.password)
            smtp.sendmail(self.sender_mail, receivers, email.as_string())
        return receivers

    def send(self, subject: str, body: str, receiver_mails: Optional[Sequence[str]] = None) -> bool:
        """Email body now, to receiver_mails or else the sender's receivers.

        Returns whether the email was sent. Either way it is logged with its
        sender and receivers, a failure with its exception, not raised.
        """
        try:
            receivers = self.send_message(subject, body, receiver_mails)
        except Exception:
            attempted = self.receiver_mails if receiver_mails is None else list(receiver_mails)
            logger.exception(f'Failed to send email {subject!r} from {self.fromaddr} '
                             f'to {", ".join(attempted) or "no receivers"}')
            return False
        logger.info(f'Sent email {subject!r} from {self.fromaddr} to {", ".join(receivers)}')
        return True
