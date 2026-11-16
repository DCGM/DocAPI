"""The server's mail senders: one per channel for the life of the process, so
their send receiver and server limits count all of the server's mail."""
from threading import Lock
from typing import Optional

from doc_api.config import config
from doc_api.tools.mail_sender import MailSender

_lock = Lock()
_senders: dict[str, Optional[MailSender]] = {}


def mail_sender(channel: str) -> Optional[MailSender]:
    """The INTERNAL or EXTERNAL sender, built from the settings on first use;
    None when the channel has no server or sender address."""
    with _lock:
        if channel not in _senders:
            _senders[channel] = config.mail_sender(channel)
        return _senders[channel]
