import logging
from email.utils import formataddr
from typing import Optional, List, Union

from metakat.app.config import config
from metakat.app.tools.mail.mail_handler import MailHandler

logger = logging.getLogger(__name__)


def get_internal_mail_logger():
    return MailLogger(logger_name='metakat.internal_mail_logger',
                      sender_mail=config.INTERNAL_MAIL_SENDER_MAIL,
                      receiver_mails=config.INTERNAL_MAIL_RECEIVER_MAILS,
                      server=config.INTERNAL_MAIL_SERVER,
                      port=config.INTERNAL_MAIL_PORT,
                      sender_name=config.INTERNAL_MAIL_SENDER_NAME,
                      password=config.INTERNAL_MAIL_PASSWORD,
                      flood_level=config.INTERNAL_MAIL_FLOOD_LEVEL)


def get_external_mail_logger():
    return MailLogger(logger_name='metakat.external_mail_logger',
                      sender_mail=config.EXTERNAL_MAIL_SENDER_MAIL,
                      server=config.EXTERNAL_MAIL_SERVER,
                      port=config.EXTERNAL_MAIL_PORT,
                      sender_name=config.EXTERNAL_MAIL_SENDER_NAME,
                      password=config.EXTERNAL_MAIL_PASSWORD,
                      flood_level=config.EXTERNAL_MAIL_FLOOD_LEVEL)


class MailLogger:

    # mail_logger.logger.critical(f'mail message', extra={'subject': 'text of subject'})
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
            subject: str = f'%(subject)s'):

        self.logger_name = logger_name
        self.sender_mail = sender_mail
        self.server = server
        self.port = port
        if receiver_mails is None:
            self.receiver_mails = []
        else:
            self.receiver_mails = receiver_mails
        self.sender_name = sender_name
        self.password = password
        self.flood_level = flood_level

        self.logger = logging.getLogger(logger_name)
        # if the logger already exists do not initialize it again
        if self.logger.hasHandlers():
            for handler in self.logger.handlers:
                if handler.name == 'mail_handler':
                    self.handler = handler
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

        self.mail_handler = None

        if self.server is None or self.sender_mail is None:
            self.logger.addHandler(logging.NullHandler())
            return

        if sender_name is not None:
            fromaddr = formataddr((sender_name, sender_mail))
        else:
            fromaddr = sender_mail

        if password is not None:
            self.mail_handler = MailHandler(fromaddr=fromaddr,
                                            toaddrs=receiver_mails,
                                            mailhost=(server, port),
                                            username=sender_mail,
                                            password=password,
                                            flood_level=flood_level,
                                            secure=True,
                                            subject=subject)
        else:
            self.mail_handler = MailHandler(fromaddr=fromaddr,
                                            toaddrs=receiver_mails,
                                            mailhost=(server, port),
                                            flood_level=flood_level,
                                            subject=subject)
        self.mail_handler.name = 'mail_handler'

        self.mail_handler.setLevel(logging.DEBUG)
        self.logger.addHandler(self.mail_handler)

    def set_receiver_mails(self, receiver_mails):
        if self.mail_handler is not None:
            self.receiver_mails = receiver_mails
            self.mail_handler.toaddrs = self.receiver_mails
        else:
            logger.warning(f'Mail handler was not initialized for {self.logger_name}, '
                           f'this is probably due to missing SMTP server or sender mail, no emails will be send!')


