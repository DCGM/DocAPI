import logging
from typing import Optional, List, Union

from doc_api.tools.mail.mail_handler import MailHandler
from doc_api.tools.mail.mail_sender import MailSender

logger = logging.getLogger(__name__)


class MailLogger:
    """A logger whose records are emailed.

    mail_logger.logger.critical('mail message', extra={'subject': 'text of subject'})

    The records go to a MailHandler, which sends them with a MailSender; the
    logger does not propagate, so they reach no other handlers. Without a
    server or a sender mail nothing is sent and the records go nowhere.
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
            security: str = 'auto',
            timeout: float = 60):

        self.logger_name = logger_name
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

        if server is None:
            logger.warning(f'SMTP server was not specified for {logger_name}, '
                           f'logging with this logger will not send any emails!')

        if sender_mail is None:
            logger.warning(f'Sender mail server was not specified for {logger_name}, '
                           f'logging with this logger will not send any emails!')

        if server is None or sender_mail is None:
            self.logger.addHandler(logging.NullHandler())
            return

        sender = MailSender(server=server,
                            sender_mail=sender_mail,
                            port=port,
                            receiver_mails=receiver_mails,
                            sender_name=sender_name,
                            username=username,
                            password=password,
                            security=security,
                            timeout=timeout)
        self.mail_handler = MailHandler(sender, subject=subject, flood_level=flood_level)
        self.mail_handler.name = 'mail_handler'

        self.mail_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.mail_handler)

    def set_receiver_mails(self, receiver_mails):
        if self.mail_handler is not None:
            self.mail_handler.sender.receiver_mails = list(receiver_mails)
        else:
            logger.warning(f'Mail handler was not initialized for {self.logger_name}, '
                           f'this is probably due to missing SMTP server or sender mail, no emails will be send!')
