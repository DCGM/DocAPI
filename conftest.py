"""Tests never send real email.

Mail settings are dropped from the environment before the application config
is imported, so no sender is built from a developer's shell or from processes
the tests start. Any SMTP connection a test does not mock itself fails the test
and never reaches a server, except to a local server from the local_smtp
fixture: an aiosmtpd server on 127.0.0.1 that keeps the emails it receives.
"""
import os
import shutil
import smtplib
import socket
import ssl
import subprocess
from dataclasses import dataclass, field
from email import message_from_bytes, policy
from email.message import EmailMessage
from typing import Optional

import pytest

for _name in [name for name in os.environ if "_MAIL_" in name]:
    del os.environ[_name]

LOCAL_HOST = "127.0.0.1"
_REAL_CONNECT = smtplib.SMTP.connect


class SmtpGuard:
    """Blocks SMTP connections except to the local servers of this test.

    SMTP.connect is where SMTP, SMTP_SSL and LMTP all open their connection.
    """

    def __init__(self):
        self.local_ports: set[int] = set()
        self.attempts: list[str] = []

    def connect(self, smtp, host="localhost", port=0, source_address=None):
        if host == LOCAL_HOST and port in self.local_ports:
            return _REAL_CONNECT(smtp, host, port, source_address)
        self.attempts.append(f"{type(smtp).__name__}({host!r}, {port!r})")
        raise OSError(f"Real SMTP is blocked in tests: {type(smtp).__name__}({host!r}, {port!r})")


@pytest.fixture(autouse=True)
def no_real_smtp(monkeypatch):
    guard = SmtpGuard()
    monkeypatch.setattr(smtplib.SMTP, "connect", lambda smtp, *args, **kwargs: guard.connect(smtp, *args, **kwargs))
    yield guard
    if guard.attempts:
        pytest.fail(f"A test tried to send real email: {', '.join(guard.attempts)}")


@dataclass
class ReceivedMail:
    mail_from: str
    rcpt_tos: list[str]
    message: EmailMessage
    login: Optional[str]  # The user the client logged in as, if it did.


@dataclass
class LocalSmtpServer:
    host: str
    port: int
    security: str
    password: Optional[str]
    ca_file: Optional[str] = None  # The certificate to trust over TLS.
    messages: list[ReceivedMail] = field(default_factory=list)


class _KeepMessages:
    def __init__(self, server: LocalSmtpServer):
        self.server = server

    async def handle_DATA(self, smtp, session, envelope):
        login = session.auth_data.decode() if session.authenticated else None
        self.server.messages.append(ReceivedMail(
            envelope.mail_from, list(envelope.rcpt_tos),
            message_from_bytes(envelope.original_content, policy=policy.default), login))
        return "250 OK"


@pytest.fixture(scope="session")
def smtp_certificate(tmp_path_factory):
    """A throwaway self-signed certificate for the local server's TLS, valid
    for 127.0.0.1; clients trust it as their CA file."""
    openssl = shutil.which("openssl")
    if openssl is None:
        pytest.skip("The openssl command is needed for a TLS test certificate")
    directory = tmp_path_factory.mktemp("smtp-tls")
    cert, key = directory / "cert.pem", directory / "key.pem"
    subprocess.run([openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                    "-subj", "/CN=localhost", "-addext", f"subjectAltName=IP:{LOCAL_HOST},DNS:localhost",
                    "-keyout", str(key), "-out", str(cert)],
                   check=True, capture_output=True)
    return cert, key


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind((LOCAL_HOST, 0))
        return probe.getsockname()[1]


@pytest.fixture
def local_smtp(no_real_smtp, request):
    """Start local SMTP servers: local_smtp(security='none', login=None, password=None).

    security is 'none', 'starttls' (required before MAIL) or 'ssl'; with TLS,
    server.ca_file is the certificate a client must trust. With a password the
    server requires login over TLS as login and password.
    """
    controller_module = pytest.importorskip("aiosmtpd.controller")
    from aiosmtpd.smtp import AuthResult, LoginPassword

    controllers = []

    def start(security: str = "none", login: Optional[str] = None, password: Optional[str] = None) -> LocalSmtpServer:
        server = LocalSmtpServer(LOCAL_HOST, _free_port(), security, password)
        kwargs = {}
        if security in ("starttls", "ssl"):
            cert, key = request.getfixturevalue("smtp_certificate")
            server.ca_file = str(cert)
            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(cert, key)
            if security == "ssl":
                # aiosmtpd only counts STARTTLS as TLS for AUTH; this connection
                # is encrypted from the start.
                kwargs.update(ssl_context=context, auth_require_tls=False)
            else:
                kwargs.update(tls_context=context, require_starttls=True)
        if password is not None:
            def authenticate(smtp, session, envelope, mechanism, auth_data):
                ok = (isinstance(auth_data, LoginPassword)
                      and auth_data.login.decode() == login and auth_data.password.decode() == password)
                return AuthResult(success=ok, auth_data=auth_data.login if ok else None)
            kwargs.update(authenticator=authenticate, auth_required=True)
        controller = controller_module.Controller(_KeepMessages(server), hostname=server.host, port=server.port,
                                                  **kwargs)
        controller.start()
        controllers.append(controller)
        no_real_smtp.local_ports.add(server.port)
        return server

    yield start
    for controller in controllers:
        controller.stop()
