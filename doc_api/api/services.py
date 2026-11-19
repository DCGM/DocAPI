"""Objects the server builds once from its settings and shares for the life of
the process: the secret masker and one mail sender per channel.

They are built when this module is first imported, at startup. One sender per
channel makes its send receiver and server limits count all of the server's
mail. Callers look them up here at call time (``services.masker``,
``services.mail_sender(channel)``), so tests can replace them."""
from typing import Optional

from doc_api.config import Config, config
from doc_api.tools.mail_sender import MailSender
from doc_api.tools.secrets import SecretMasker

MAIL_CHANNELS = ("INTERNAL", "EXTERNAL")


def build_masker(settings: Config) -> SecretMasker:
    """A masker with the SECRET_MASKING_* settings."""
    return SecretMasker(enabled=settings.SECRET_MASKING, mask=settings.SECRET_MASKING_MASK,
                        allowed_keys=settings.SECRET_MASKING_ALLOWED_KEYS,
                        high_entropy=settings.SECRET_MASKING_HIGH_ENTROPY,
                        partial_keys=settings.SECRET_MASKING_PARTIAL_KEYS,
                        partial_start=settings.SECRET_MASKING_PARTIAL_START,
                        partial_end=settings.SECRET_MASKING_PARTIAL_END)


def build_mail_sender(settings: Config, channel: str) -> Optional[MailSender]:
    """A sender for the INTERNAL or EXTERNAL mail channel; None when the channel
    has no server or sender address. Callers send to each receiver, internal
    mail to INTERNAL_MAIL_RECEIVER_MAILS."""
    if channel not in MAIL_CHANNELS:
        raise ValueError(f"Unknown mail channel: {channel}")
    server = getattr(settings, f"{channel}_MAIL_SERVER")
    sender_mail = getattr(settings, f"{channel}_MAIL_SENDER_MAIL")
    if not server or not sender_mail:
        return None
    return MailSender(server=server, sender_mail=sender_mail,
                      port=getattr(settings, f"{channel}_MAIL_PORT"),
                      sender_name=getattr(settings, f"{channel}_MAIL_SENDER_NAME"),
                      username=getattr(settings, f"{channel}_MAIL_USERNAME"),
                      password=getattr(settings, f"{channel}_MAIL_PASSWORD"),
                      security=getattr(settings, f"{channel}_MAIL_SECURITY"),
                      ca_file=getattr(settings, f"{channel}_MAIL_CA_FILE"),
                      timeout=getattr(settings, f"{channel}_MAIL_TIMEOUT_SECONDS"),
                      send_receiver_limit=getattr(settings, f"{channel}_MAIL_SEND_RECEIVER_LIMIT"),
                      send_receiver_window_seconds=getattr(settings, f"{channel}_MAIL_SEND_RECEIVER_WINDOW_SECONDS"),
                      send_server_limit=getattr(settings, f"{channel}_MAIL_SEND_SERVER_LIMIT"),
                      send_server_window_seconds=getattr(settings, f"{channel}_MAIL_SEND_SERVER_WINDOW_SECONDS"))


masker: SecretMasker = build_masker(config)
mail_senders: dict[str, Optional[MailSender]] = {
    channel: build_mail_sender(config, channel) for channel in MAIL_CHANNELS}


def mail_sender(channel: str) -> Optional[MailSender]:
    """The server's INTERNAL or EXTERNAL sender; None when the channel has no
    server or sender address."""
    if channel not in MAIL_CHANNELS:
        raise ValueError(f"Unknown mail channel: {channel}")
    return mail_senders[channel]
