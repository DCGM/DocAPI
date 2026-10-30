import logging
from email.utils import formataddr
from typing import Optional, List, Sequence, Union

from doc_api.tools.mail.mail_handler import MailHandler

logger = logging.getLogger(__name__)

SECURITY_MODES = ('auto', 'none', 'ssl', 'starttls')


# The server's own config is imported only by its factories, so that a worker
# or a client can use MailLogger without the server's settings.
def get_internal_mail_logger():
    from doc_api.config import config
    return MailLogger(logger_name='doc_api.internal_mail_logger',
                      sender_mail=config.INTERNAL_MAIL_SENDER_MAIL,
                      receiver_mails=config.INTERNAL_MAIL_RECEIVER_MAILS,
                      server=config.INTERNAL_MAIL_SERVER,
                      port=config.INTERNAL_MAIL_PORT,
                      sender_name=config.INTERNAL_MAIL_SENDER_NAME,
                      password=config.INTERNAL_MAIL_PASSWORD,
                      flood_level=config.INTERNAL_MAIL_FLOOD_LEVEL)


def get_external_mail_logger():
    from doc_api.config import config
    return MailLogger(logger_name='doc_api.external_mail_logger',
                      sender_mail=config.EXTERNAL_MAIL_SENDER_MAIL,
                      server=config.EXTERNAL_MAIL_SERVER,
                      port=config.EXTERNAL_MAIL_PORT,
                      sender_name=config.EXTERNAL_MAIL_SENDER_NAME,
                      password=config.EXTERNAL_MAIL_PASSWORD,
                      flood_level=config.EXTERNAL_MAIL_FLOOD_LEVEL)


class MailLogger:
    """A logger whose records are emailed, and a sender of single emails.

    Logged:   mail_logger.logger.critical('mail message', extra={'subject': 'text of subject'})
    Direct:   mail_logger.send('text of subject', 'mail message', ['user@mail.server.cz'])

    security: 'ssl' connects over SSL, 'starttls' upgrades a plain connection,
    'none' sends in plain text and 'auto' is 'ssl' with a password, 'none'
    without. The server is logged in to as username (sender_mail by default)
    whenever a password is given.

    Without a server or a sender mail nothing is sent: logging goes nowhere
    and send() returns False.
    """

    def __init__(
            self,
            logger_name: str,
            sender_mail: Union[str, None],
            server: Union[str, None],
            port: int = 25,
            receiver_mails: Optional[List[str]] = None,
            sender_name: Optional[str] = None,
            password: Optional[str] = None,
            flood_level: int = 1000000000000000,
            subject: str = f'%(subject)s',
            username: Optional[str] = None,
            security: str = 'auto'):

        if security not in SECURITY_MODES:
            raise ValueError(f'Unknown mail security {security!r}, expected one of {", ".join(SECURITY_MODES)}')
        if security == 'auto':
            security = 'ssl' if password is not None else 'none'

        self.logger_name = logger_name
        self.sender_mail = sender_mail
        self.server = server
        self.port = int(port)
        self.receiver_mails = list(receiver_mails or [])
        self.sender_name = sender_name
        self.password = password
        self.username = username or sender_mail
        self.security = security
        self.flood_level = flood_level
        self.mail_handler = None

        self.logger = logging.getLogger(logger_name)
        # if the logger already exists do not initialize it again
        if self.logger.hasHandlers():
            for handler in self.logger.handlers:
                if handler.name == 'mail_handler':
                    self.mail_handler = handler
                    return

        self.logger.handlers.clear()

        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        if self.server is None:
            logger.warning(f'SMTP server was not specified for {logger_name}, '
                           f'logging with this logger will not send any emails!')

        if self.sender_mail is None:
            logger.warning(f'Sender mail server was not specified for {logger_name}, '
                           f'logging with this logger will not send any emails!')

        if self.server is None or self.sender_mail is None:
            self.logger.addHandler(logging.NullHandler())
            return

        if sender_name is not None:
            fromaddr = formataddr((sender_name, sender_mail))
        else:
            fromaddr = sender_mail

        self.mail_handler = MailHandler(fromaddr=fromaddr,
                                        toaddrs=self.receiver_mails,
                                        mailhost=(server, self.port),
                                        username=self.username if password is not None else None,
                                        password=password,
                                        flood_level=flood_level,
                                        secure=security == 'ssl',
                                        starttls=security == 'starttls',
                                        subject=subject)
        self.mail_handler.name = 'mail_handler'

        self.mail_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.mail_handler)

    @property
    def enabled(self) -> bool:
        """Whether this logger can send emails at all."""
        return self.mail_handler is not None

    def set_receiver_mails(self, receiver_mails):
        if self.mail_handler is not None:
            self.receiver_mails = list(receiver_mails)
            self.mail_handler.toaddrs = self.receiver_mails
        else:
            logger.warning(f'Mail handler was not initialized for {self.logger_name}, '
                           f'this is probably due to missing SMTP server or sender mail, no emails will be send!')

    def send(self, subject: str, message: str, receiver_mails: Optional[Sequence[str]] = None) -> bool:
        """Email message now, to receiver_mails or else the logger's receivers.

        Returns whether the email was sent; a failure is logged, not raised.
        Unlike logged messages, sent emails do not count towards the flood level.
        """
        if self.mail_handler is None:
            logger.warning(f'Not sending email {subject!r}: {self.logger_name} has no SMTP server or sender mail')
            return False
        receivers = self.receiver_mails if receiver_mails is None else list(receiver_mails)
        if not receivers:
            logger.warning(f'Not sending email {subject!r}: no receiver mails')
            return False
        try:
            self.mail_handler.send_message(subject, message, receivers)
        except Exception:
            logger.exception(f'Failed to send email {subject!r} to {", ".join(receivers)}')
            return False
        return True
