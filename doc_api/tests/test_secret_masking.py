import copy
import io
import logging

import pytest

from doc_api.api.checkers import job_mail
from doc_api.api.schemas.base_objects import ProcessingState
from doc_api.config import Config, config
from doc_api.db import model
from doc_api.tools.secrets import MaskingSettings, SecretMaskingFilter, mask_secrets_in_struct, mask_secrets_in_text

MASK = "*****"
OPENAI_KEY = "sk-proj-abcdefghijklmnopqrstuvwx"
OPENAI_KEY_PARTLY = "sk-proj-*****uvwx"
SETTINGS = {
    "biblio": {"core": {"vlm": {"api_key": OPENAI_KEY, "model": "gpt-4o", "api_key_env": "OPENAI_API_KEY",
                                "max_tokens": 512}}},
    "Auth": {"user": "reader", "password": "hunter2"},
    "services": [{"name": "ocr", "Access-Token": "abc"}, {"name": "llm", "token": None}],
    "author": "Jan Novák",
    "keywords": ["history"],
    "endpoint": "https://reader:hunter2@llm.example.org/v1",
    "note": f"fallback key is {OPENAI_KEY}",
}


@pytest.fixture(autouse=True)
def masking_settings(monkeypatch):
    """Each test starts from the default settings; monkeypatch restores them."""
    monkeypatch.setattr(config, "SECRET_MASKING", True)
    monkeypatch.setattr(config, "SECRET_MASKING_ALLOWED_KEYS", [])
    monkeypatch.setattr(config, "SECRET_MASKING_HIGH_ENTROPY", False)
    monkeypatch.setattr(config, "SECRET_MASKING_MASK", MASK)
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_KEYS", [])
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_START", 8)
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_END", 4)
    monkeypatch.setattr(config, "SECRET_MASKING_INTERNAL_MAIL", True)
    monkeypatch.setattr(config, "SECRET_MASKING_EXTERNAL_MAIL", False)



def masked_text(text, settings=None):
    """Masked with the server's settings in force, as the server masks."""
    return mask_secrets_in_text(text, settings or config.secret_masking())


def masked_struct(value, settings=None):
    return mask_secrets_in_struct(value, settings or config.secret_masking())


def test_mask_secrets_in_struct_nested_keys_and_subtrees():
    original = copy.deepcopy(SETTINGS)
    masked = masked_struct(SETTINGS)
    assert SETTINGS == original
    assert masked == {
        "biblio": {"core": {"vlm": {"api_key": MASK, "model": "gpt-4o", "api_key_env": MASK,
                                    "max_tokens": MASK}}},
        "Auth": MASK,
        "services": [{"name": "ocr", "Access-Token": MASK}, {"name": "llm", "token": None}],
        "author": MASK,
        "keywords": MASK,
        "endpoint": f"https://reader:{MASK}@llm.example.org/v1",
        "note": f"fallback key is {MASK}",
    }
    assert masked_struct(masked) == masked
    assert masked_struct(("x", 1, None)) == ("x", 1, None)


def test_mask_secrets_in_struct_allowlist(monkeypatch):
    settings = {"api_key_env": "sk-abcdefghijklmnopqrstuvwx", "session": "s1", "cache_key": "pages", "author": "A"}
    assert masked_struct(settings) == {"api_key_env": MASK, "session": MASK, "cache_key": MASK, "author": MASK}
    monkeypatch.setattr(config, "SECRET_MASKING_ALLOWED_KEYS", ["api_key_env", "Cache-Key", " AUTHOR "])
    # Allowed values are still scanned for secret patterns.
    assert masked_struct(settings) == \
           {"api_key_env": MASK, "session": MASK, "cache_key": "pages", "author": "A"}
    assert masked_text("author=A, cache-key=pages, session=s1") == "author=A, cache-key=pages, session=*****"
    assert mask_secrets_in_struct(settings, MaskingSettings(allowed_keys=["session"])) == \
           {"api_key_env": MASK, "session": "s1", "cache_key": MASK, "author": MASK}


@pytest.mark.parametrize("text,expected", [
    (f"key {OPENAI_KEY}", "key *****"),
    ("sk-or-v1-0123456789abcdefghijklmnop", "*****"),
    ("Authorization: Bearer abcdefgh.ijk-lmnop", "Authorization: *****"),
    ("curl -H 'Bearer abcdefghijkl'", "curl -H 'Bearer *****'"),
    ("token ghp_abcdefghijklmnopqrstuvwxyz0123456789", "token *****"),
    ("aws AKIAIOSFODNN7EXAMPLE", "aws *****"),
    ("hf_abcdefghijklmnopqrstuvwxyzABCDEF", "*****"),
    ("jwt eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjMifQ.c2lnbmF0dXJl", "jwt *****"),
    ("X-API-Key: doc-api.kid123.longsecretvalue", "X-API-Key: *****"),
    ("postgresql+asyncpg://user:pa55@db:5432/postgres", "postgresql+asyncpg://user:*****@db:5432/postgres"),
    ("GET /v1/x?api_key=abc&page=2", "GET /v1/x?api_key=*****&page=2"),
    ('{"password": "p\\"w", "user": "u"}', '{"password": "*****", "user": "u"}'),
    ("JobLease(engine_settings={'api_key': 'x', 'n': 1})", "JobLease(engine_settings={'api_key': '*****', 'n': 1})"),
    ('{\\"secret\\": \\"x\\"}', '{\\"secret\\": \\"*****\\"}'),
    ("client_secret=None", "client_secret=None"),
    ("KeyError: 'images'", "KeyError: '*****'"),
    ("ValueError: password=hunter2", "ValueError: password=*****"),
    ("Detail: x-api-key:abc", "Detail: x-api-key:*****"),
    ("Admin API key created!", "Admin API key created!"),
    ("Status: 403, Code: API_KEY_INVALID", "Status: 403, Code: API_KEY_INVALID"),
    ("Engine: name=ocr, version=1.0, author=X", "Engine: name=ocr, version=1.0, author=*****"),
])
def test_mask_secrets_in_text_patterns(text, expected):
    assert masked_text(text) == expected
    assert masked_struct(text) == expected


@pytest.mark.parametrize("prefix", ["doc-api", "myapp"])
def test_dotted_names_with_key_prefix_are_not_masked(monkeypatch, prefix):
    monkeypatch.setattr(config, "KEY_PREFIX", prefix)
    # A key prefix that is also a package or host name leaves those names alone.
    for text in (f"{prefix}.engines.loader.bind INFO : Loaded",
                 f"https://{prefix}.api.example.org/v1/jobs",
                 "doc-api.tools.secrets imported"):
        assert masked_text(text) == text
    # Labelled keys stay masked whatever the prefix.
    key = f"{prefix}.Ab3dE9xY.0123456789abcdefghijABCDEFGHIJ0123456789"
    assert masked_text(f"X-API-Key: {key}") == "X-API-Key: *****"
    assert masked_text(f"GET /v1/me?api_key={key}&page=2") == "GET /v1/me?api_key=*****&page=2"
    assert masked_text(f'{{"api_key": "{key}"}}') == '{"api_key": "*****"}'
    assert masked_struct({"worker": {"api_key": key}}) == {"worker": {"api_key": MASK}}


def test_custom_mask(monkeypatch):
    monkeypatch.setattr(config, "SECRET_MASKING_MASK", "[hidden]")
    assert masked_struct({"api_key": "x", "n": 1}) == {"api_key": "[hidden]", "n": 1}
    assert masked_text(f"api_key={OPENAI_KEY}, note {OPENAI_KEY}") == "api_key=[hidden], note [hidden]"


def test_mask_secrets_in_text_high_entropy_switch(monkeypatch):
    text = "token-free id Q2xhdWRlIGNvZGUgaXMgaGVyZQ1234abcdXYZ and sha " + "9f86d081884c7d65" * 4
    assert masked_text(text) == text
    masked = "token-free id ***** and sha " + "9f86d081884c7d65" * 4
    assert mask_secrets_in_text(text, MaskingSettings(high_entropy=True)) == masked
    monkeypatch.setattr(config, "SECRET_MASKING_HIGH_ENTROPY", True)
    assert masked_text(text) == masked


def _masked_log(*, propagate_from: str = "doc_api.tests.secret_masking"):
    """A handler set up like the server's: base formatter and masking filter."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(levelname)s : %(message)s"))
    handler.addFilter(SecretMaskingFilter(settings=config.secret_masking))
    logger = logging.getLogger(propagate_from)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger, handler, stream


def test_server_log_handlers_mask_secrets(monkeypatch):
    server = Config()
    handlers = server.LOGGING_CONFIG["handlers"]
    mask_filter = server.LOGGING_CONFIG["filters"]["mask_secrets"]
    assert mask_filter["()"] == "doc_api.tools.secrets.SecretMaskingFilter"
    assert all(handler["filters"] == ["mask_secrets"] for handler in handlers.values())
    # The filter reads the server's settings in force for each record.
    monkeypatch.setattr(server, "SECRET_MASKING_MASK", "[hidden]")
    assert mask_filter["settings"]() == server.secret_masking()
    assert mask_filter["settings"]().mask == "[hidden]"


def test_logging_masks_only_exceptions():
    logger, handler, stream = _masked_log()
    try:
        # DocAPI never logs secrets itself: plain records pass unchanged.
        logger.info("settings %s retries %d", {"api_key": OPENAI_KEY}, 3)
        try:
            raise RuntimeError(f"rejected {OPENAI_KEY}, password=hunter2")
        except RuntimeError:
            logger.exception("call failed for %s", OPENAI_KEY)
        # Third-party exceptions propagating to the handler are masked too.
        try:
            raise ValueError("[parameters: ('sk-or-v1-0123456789abcdefghijklmnop',)]")
        except ValueError:
            logging.getLogger("doc_api.tests.secret_masking.sqlalchemy").error("statement failed", exc_info=True)
    finally:
        logger.removeHandler(handler)
    text = stream.getvalue()
    assert f"settings {{'api_key': '{OPENAI_KEY}'}} retries 3" in text
    assert "call failed for *****" in text
    assert "RuntimeError: rejected *****, password=*****" in text
    assert "ValueError: [parameters: ('*****',)]" in text
    assert text.count(OPENAI_KEY) == 1

    record = logging.LogRecord("x", logging.INFO, __file__, 1, "password=%s", ("hunter2",), None)
    assert SecretMaskingFilter().filter(record) is True
    assert record.getMessage() == "password=hunter2"


def _access_record(path):
    """An access record shaped like uvicorn's."""
    return logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d',
                             ("10.0.0.1:5000", "GET", path, "1.1", 401), None)


def test_access_log_masks_query_string_keys():
    key = "doc-api.Ab3dE9xY.0123456789abcdefghijABCDEFGHIJ0123456789"
    record = _access_record(f"/v1/me?api_key={key}&limit=5")
    assert SecretMaskingFilter().filter(record) is True
    assert record.getMessage() == '10.0.0.1:5000 - "GET /v1/me?api_key=*****&limit=5 HTTP/1.1" 401'
    # Paths without a query string are not scanned; plain queries pass unchanged.
    for path in ("/v1/jobs/lease", "/v1/jobs?state=done&limit=50"):
        record = _access_record(path)
        SecretMaskingFilter().filter(record)
        assert record.args[2] == path
    # Only the access logger gets this; other records still need an exception.
    record = logging.LogRecord("uvicorn.error", logging.INFO, __file__, 1, "%s", (f"?api_key={key}",), None)
    SecretMaskingFilter().filter(record)
    assert key in record.getMessage()
    # Through a real handler, as uvicorn's records reach the server's handlers.
    logger, handler, stream = _masked_log(propagate_from="uvicorn.access")
    propagate, logger.propagate = logger.propagate, False
    try:
        logger.info('%s - "%s %s HTTP/%s" %d', "10.0.0.1:5000", "GET", f"/v1/me?api_key={key}", "1.1", 200)
    finally:
        logger.removeHandler(handler)
        logger.propagate = propagate
    assert stream.getvalue() == 'INFO : 10.0.0.1:5000 - "GET /v1/me?api_key=***** HTTP/1.1" 200\n'


@pytest.mark.asyncio
async def test_unhandled_error_masks_url_and_mail(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from doc_api.api import main
    from doc_api.config import config
    monkeypatch.setattr(config, "INTERNAL_MAIL_SERVER", "smtp.invalid")
    mail_logger = Mock()
    monkeypatch.setattr(main, "internal_mail_logger", mail_logger)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    main.exception_logger.addHandler(handler)
    request = SimpleNamespace(url=f"http://test/v1/jobs?token={OPENAI_KEY}", method="POST", client="client")
    try:
        try:
            raise RuntimeError(f"could not parse api_key={OPENAI_KEY}")
        except RuntimeError as exc:
            response = await main.unhandled(request, exc)
    finally:
        main.exception_logger.removeHandler(handler)
    assert response.status_code == 500
    mail = mail_logger.critical.call_args.args[0]
    assert OPENAI_KEY not in mail
    assert "URL: http://test/v1/jobs?token=*****" in mail
    assert "ERROR: could not parse api_key=*****" in mail
    assert "URL: http://test/v1/jobs?token=*****" in stream.getvalue()


@pytest.mark.asyncio
async def test_unhandled_error_mail_unmasked_when_internal_mail_masking_off(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock
    from doc_api.api import main
    monkeypatch.setattr(config, "SECRET_MASKING_INTERNAL_MAIL", False)
    monkeypatch.setattr(config, "INTERNAL_MAIL_SERVER", "smtp.invalid")
    mail_logger = Mock()
    monkeypatch.setattr(main, "internal_mail_logger", mail_logger)
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    main.exception_logger.addHandler(handler)
    request = SimpleNamespace(url=f"http://test/v1/jobs?token={OPENAI_KEY}", method="POST", client="client")
    try:
        try:
            raise RuntimeError("failed")
        except RuntimeError as exc:
            await main.unhandled(request, exc)
    finally:
        main.exception_logger.removeHandler(handler)
    assert f"URL: http://test/v1/jobs?token={OPENAI_KEY}" in mail_logger.critical.call_args.args[0]
    # The server log is masked regardless of the mail setting.
    assert "URL: http://test/v1/jobs?token=*****" in stream.getvalue()


def _sent_report(outbox, subject, body, channel="INTERNAL"):
    job_mail.send_final_report(job_mail.JobMail(channel, ("admin@example.com",), subject, body))
    message = outbox[-1][2]
    return message["Subject"], message.get_payload(decode=True).decode()


def test_masking_can_be_disabled(monkeypatch, job_mail_outbox):
    monkeypatch.setattr(config, "SECRET_MASKING", False)
    assert masked_struct(SETTINGS) == SETTINGS
    assert masked_text(f"api_key={OPENAI_KEY}") == f"api_key={OPENAI_KEY}"
    logger, handler, stream = _masked_log()
    try:
        try:
            raise RuntimeError(OPENAI_KEY)
        except RuntimeError:
            logger.exception("key %s", OPENAI_KEY)
    finally:
        logger.removeHandler(handler)
    assert f"key {OPENAI_KEY}" in stream.getvalue()
    assert f"RuntimeError: {OPENAI_KEY}" in stream.getvalue()
    assert _sent_report(job_mail_outbox, "Job", f"api_key={OPENAI_KEY}")[1] == f"api_key={OPENAI_KEY}"


def test_partial_keys_in_struct(monkeypatch):
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_KEYS", ["API-Key", "model"])
    settings = {"vlm": {"api_key": OPENAI_KEY, "model": "gpt-4o-mini-2024-07-18-preview", "token": OPENAI_KEY},
                "short": {"api_key": "sk-abc"}, "nested": {"api_key": {"value": OPENAI_KEY}},
                "number": {"model": 4}, "unset": {"api_key": None}}
    assert masked_struct(settings) == {
        # Partial keys override: a key that does not look secret is masked too.
        "vlm": {"api_key": OPENAI_KEY_PARTLY, "model": "gpt-4o-m*****view", "token": MASK},
        # Too short to keep half hidden, and not a string: masked fully.
        "short": {"api_key": MASK}, "nested": {"api_key": MASK}, "number": {"model": MASK},
        "unset": {"api_key": None},
    }
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_START", 3)
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_END", 0)
    assert masked_struct({"api_key": OPENAI_KEY}) == {"api_key": "sk-*****"}


@pytest.mark.parametrize("text, expected", [
    (f"GET /v1/x?api_key={OPENAI_KEY}&page=2", f"GET /v1/x?api_key={OPENAI_KEY_PARTLY}&page=2"),
    (f'{{"api_key": "{OPENAI_KEY}", "token": "{OPENAI_KEY}"}}',
     f'{{"api_key": "{OPENAI_KEY_PARTLY}", "token": "*****"}}'),
    (f"{{'api-key': '{OPENAI_KEY}'}}", f"{{'api-key': '{OPENAI_KEY_PARTLY}'}}"),
    (f'{{\\"api_key\\": \\"{OPENAI_KEY}\\"}}', f'{{\\"api_key\\": \\"{OPENAI_KEY_PARTLY}\\"}}'),
    (f"ValueError: api_key={OPENAI_KEY}", f"ValueError: api_key={OPENAI_KEY_PARTLY}"),
    (f"api_key: Bearer {OPENAI_KEY}", f"api_key: Bearer {OPENAI_KEY_PARTLY}"),
    ("api_key=sk-abc", "api_key=*****"),
    ("api_key=None", "api_key=None"),
    # Outside a partial-key pair the general masking applies.
    (f"fallback {OPENAI_KEY}", "fallback *****"),
])
def test_partial_keys_in_text(monkeypatch, text, expected):
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_KEYS", ["api_key"])
    assert masked_text(text) == expected


def test_partial_keys_apply_with_masking_disabled(monkeypatch, job_mail_outbox):
    monkeypatch.setattr(config, "SECRET_MASKING", False)
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_KEYS", ["api_key"])
    assert masked_struct({"api_key": OPENAI_KEY, "token": OPENAI_KEY}) == \
           {"api_key": OPENAI_KEY_PARTLY, "token": OPENAI_KEY}
    assert masked_text(f"api_key={OPENAI_KEY} token={OPENAI_KEY}") == \
           f"api_key={OPENAI_KEY_PARTLY} token={OPENAI_KEY}"
    logger, handler, stream = _masked_log()
    try:
        try:
            raise RuntimeError(f"api_key={OPENAI_KEY}")
        except RuntimeError:
            logger.exception("failed")
    finally:
        logger.removeHandler(handler)
    assert f"RuntimeError: api_key={OPENAI_KEY_PARTLY}" in stream.getvalue()
    assert OPENAI_KEY not in stream.getvalue()
    assert _sent_report(job_mail_outbox, "Job", f"api_key={OPENAI_KEY}")[1] == f"api_key={OPENAI_KEY_PARTLY}"


def test_masking_configuration(monkeypatch):
    for name in ("SECRET_MASKING", "SECRET_MASKING_ALLOWED_KEYS", "SECRET_MASKING_HIGH_ENTROPY",
                 "SECRET_MASKING_MASK", "SECRET_MASKING_PARTIAL_KEYS", "SECRET_MASKING_PARTIAL_START",
                 "SECRET_MASKING_PARTIAL_END", "SECRET_MASKING_INTERNAL_MAIL", "SECRET_MASKING_EXTERNAL_MAIL"):
        monkeypatch.delenv(name, raising=False)
    defaults = Config()
    assert (defaults.SECRET_MASKING, defaults.SECRET_MASKING_ALLOWED_KEYS, defaults.SECRET_MASKING_HIGH_ENTROPY,
            defaults.SECRET_MASKING_MASK) == (True, [], False, "*****")
    assert (defaults.SECRET_MASKING_PARTIAL_KEYS, defaults.SECRET_MASKING_PARTIAL_START,
            defaults.SECRET_MASKING_PARTIAL_END) == ([], 8, 4)
    assert (defaults.SECRET_MASKING_INTERNAL_MAIL, defaults.SECRET_MASKING_EXTERNAL_MAIL) == (True, False)
    # The server's defaults are the masking tool's own.
    assert defaults.secret_masking() == MaskingSettings()
    monkeypatch.setenv("SECRET_MASKING", "false")
    monkeypatch.setenv("SECRET_MASKING_ALLOWED_KEYS", " api_key_env, max_tokens ,,")
    monkeypatch.setenv("SECRET_MASKING_HIGH_ENTROPY", "true")
    monkeypatch.setenv("SECRET_MASKING_MASK", "[hidden]")
    monkeypatch.setenv("SECRET_MASKING_PARTIAL_KEYS", "api_key, , vlm_key")
    monkeypatch.setenv("SECRET_MASKING_PARTIAL_START", "4")
    monkeypatch.setenv("SECRET_MASKING_PARTIAL_END", "-1")
    monkeypatch.setenv("SECRET_MASKING_INTERNAL_MAIL", "false")
    monkeypatch.setenv("SECRET_MASKING_EXTERNAL_MAIL", "true")
    configured = Config()
    assert (configured.SECRET_MASKING, configured.SECRET_MASKING_ALLOWED_KEYS,
            configured.SECRET_MASKING_HIGH_ENTROPY, configured.SECRET_MASKING_MASK) == \
           (False, ["api_key_env", "max_tokens"], True, "[hidden]")
    assert (configured.SECRET_MASKING_PARTIAL_KEYS, configured.SECRET_MASKING_PARTIAL_START,
            configured.SECRET_MASKING_PARTIAL_END) == (["api_key", "vlm_key"], 4, 0)
    assert (configured.SECRET_MASKING_INTERNAL_MAIL, configured.SECRET_MASKING_EXTERNAL_MAIL) == (False, True)
    assert configured.secret_masking() == MaskingSettings(
        enabled=False, mask="[hidden]", allowed_keys=["api_key_env", "max_tokens"], high_entropy=True,
        partial_keys=["api_key", "vlm_key"], partial_start=4, partial_end=0)
    monkeypatch.setenv("SECRET_MASKING_MASK", "")
    assert Config().SECRET_MASKING_MASK == "*****"


@pytest.mark.parametrize("channel, masked", [("INTERNAL", True), ("EXTERNAL", False)])
def test_job_report_mail_masking_defaults_per_channel(job_mail_outbox, channel, masked):
    subject, body = f"Job failed with {OPENAI_KEY}", f"Traceback: api_key={OPENAI_KEY}"
    expected = ("Job failed with *****", "Traceback: api_key=*****") if masked else (subject, body)
    assert _sent_report(job_mail_outbox, subject, body, channel) == expected


@pytest.mark.parametrize("channel", ["INTERNAL", "EXTERNAL"])
def test_job_report_mail_masking_per_channel_switch(monkeypatch, job_mail_outbox, channel):
    monkeypatch.setattr(config, "SECRET_MASKING_INTERNAL_MAIL", channel == "EXTERNAL")
    monkeypatch.setattr(config, "SECRET_MASKING_EXTERNAL_MAIL", channel == "EXTERNAL")
    body = f"api_key={OPENAI_KEY}"
    expected = "api_key=*****" if channel == "EXTERNAL" else body
    assert _sent_report(job_mail_outbox, "Job", body, channel)[1] == expected


def test_job_report_mail_is_masked(job_mail_outbox):
    assert _sent_report(job_mail_outbox, f"Job failed with {OPENAI_KEY}", f"Traceback: api_key={OPENAI_KEY}") == \
           ("Job failed with *****", "Traceback: api_key=*****")


@pytest.mark.parametrize("reader_role", ["user", "readonly", "worker", "admin", "manager"])
@pytest.mark.asyncio
async def test_get_job_200_engine_settings_masked_for_readers(client, reader_headers, reader_role, job_factory,
                                                              engine_factory, db_sessions):
    engine = await engine_factory(name="masking-engine")
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.engine_id = engine.id
        job.definition = {"images": [], "engine_settings": copy.deepcopy(SETTINGS)}
        job.previous_attempts = 1
        job.log = {'1': f"worker dump api_key={OPENAI_KEY}"}
        job.log_user = {'0': f"old {OPENAI_KEY}", '1': f"using {OPENAI_KEY}"}
    responses = [await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)]
    if reader_role != "worker":
        responses.append(await client.get("/v1/jobs", headers=reader_headers, params={"include_logs": True, "include_engine_settings": True}))
    for response in responses:
        assert response.status_code == 200, response.text
        data = response.json()["data"]
        if isinstance(data, list):
            data = next(item for item in data if item["id"] == str(job_id))
        privileged = reader_role in {"admin", "manager", "worker"}
        assert data["engine_settings"] == (SETTINGS if privileged else masked_struct(SETTINGS))
        if reader_role in {"admin", "manager"}:
            assert data["log_user"] == f"old {OPENAI_KEY}\n\nusing {OPENAI_KEY}"
        else:
            assert data["log_user"] == (f"using {OPENAI_KEY}" if privileged else "using *****")
        assert data.get("log") == (f"worker dump api_key={OPENAI_KEY}" if privileged else None)
    async with db_sessions() as db:
        job = await db.get(model.Job, job_id)
        assert job.definition["engine_settings"] == SETTINGS
        assert job.log_user == {'0': f"old {OPENAI_KEY}", '1': f"using {OPENAI_KEY}"}


@pytest.mark.parametrize("reader_role", ["user", "admin", "manager"])
@pytest.mark.asyncio
async def test_get_job_200_partial_keys_revealed_partly(monkeypatch, client, reader_headers, reader_role, job_factory,
                                                        engine_factory, db_sessions):
    monkeypatch.setattr(config, "SECRET_MASKING_PARTIAL_KEYS", ["api_key"])
    engine = await engine_factory(name="partial-masking-engine")
    job_id = await job_factory(ProcessingState.PROCESSING)
    async with db_sessions.begin() as db:
        job = await db.get(model.Job, job_id)
        job.engine_id = engine.id
        job.definition = {"images": [], "engine_settings": {"vlm": {"api_key": OPENAI_KEY, "token": OPENAI_KEY}}}
        job.log_user = {'0': f"calling api_key={OPENAI_KEY}"}
    response = await client.get(f"/v1/jobs/{job_id}", headers=reader_headers)
    assert response.status_code == 200, response.text
    data = response.json()["data"]
    if reader_role in {"admin", "manager"}:
        assert data["engine_settings"] == {"vlm": {"api_key": OPENAI_KEY, "token": OPENAI_KEY}}
    else:
        assert data["engine_settings"] == {"vlm": {"api_key": OPENAI_KEY_PARTLY, "token": MASK}}
    assert data["log_user"] == f"calling api_key={OPENAI_KEY if reader_role in {'admin', 'manager'} else OPENAI_KEY_PARTLY}"


@pytest.mark.asyncio
async def test_post_job_422_masks_echoed_input(client, user_headers):
    response = await client.post("/v1/jobs", headers=user_headers,
                                 json={"images": [], "api_key": OPENAI_KEY,
                                       "engine_settings": [f"api_key={OPENAI_KEY}"]})
    assert response.status_code == 422, response.text
    assert OPENAI_KEY not in response.text


def test_workers_and_clients_do_not_mask():
    import subprocess
    import sys
    from pathlib import Path
    # Workers and clients never load masking: what they receive, log, mail and
    # report is left as it is.
    for module in ("doc_client.doc_client_wrapper", "doc_worker.doc_worker_wrapper"):
        code = f"import sys, {module}; print('doc_api.tools.secrets' in sys.modules)"
        result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                                cwd=Path(__file__).parents[2])
        assert result.stdout.strip() == "False", module


@pytest.mark.parametrize("module", ["doc_api.tools.secrets", "doc_api.tools.mail.mail_sender",
                                    "doc_api.tools.mail.mail_handler", "doc_api.tools.mail.mail_logger",
                                    "doc_api.adapter", "doc_api.api.schemas.base_objects",
                                    "doc_worker.doc_worker_wrapper", "doc_client.doc_client_wrapper"])
def test_tools_workers_and_clients_do_not_load_server_config(module):
    import subprocess
    import sys
    from pathlib import Path
    code = f"import sys, {module}; print('doc_api.config' in sys.modules)"
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                            cwd=Path(__file__).parents[2])
    assert result.stdout.strip() == "False", module


def test_masking_tool_without_settings_uses_defaults():
    assert MaskingSettings() == MaskingSettings(enabled=True, mask="*****", allowed_keys=(), high_entropy=False,
                                                partial_keys=(), partial_start=8, partial_end=4)
    assert mask_secrets_in_text(f"api_key={OPENAI_KEY}") == "api_key=*****"
    assert mask_secrets_in_text(f"api_key={OPENAI_KEY}", MaskingSettings(enabled=False)) == f"api_key={OPENAI_KEY}"
    settings = MaskingSettings(mask="[x]", allowed_keys=[" Cache-Key "], partial_keys=["api_key"], partial_start=3,
                               partial_end=0)
    assert mask_secrets_in_struct({"cache_key": "pages", "api_key": OPENAI_KEY, "token": "t"}, settings) == \
           {"cache_key": "pages", "api_key": "sk-[x]", "token": "[x]"}


@pytest.mark.asyncio
async def test_database_password_is_never_logged(monkeypatch, caplog):
    from unittest.mock import AsyncMock, Mock
    from doc_api.config import config
    from doc_api.db import db_create
    engine = Mock(dispose=AsyncMock())
    engine.connect.side_effect = ConnectionRefusedError("refused")
    monkeypatch.setattr(db_create, "create_async_engine", Mock(return_value=engine))
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql+asyncpg://docapi:S3cr3t-pa55@db:5432/doc_api_db")
    with caplog.at_level(logging.ERROR, logger=db_create.logger.name), pytest.raises(ConnectionRefusedError):
        await db_create.create_database_if_does_not_exist()
    # The engine itself still receives the real password.
    assert "S3cr3t-pa55" in db_create.create_async_engine.call_args.args[0]
    assert "docapi:***@db:5432/postgres" in caplog.text
    assert "S3cr3t-pa55" not in caplog.text
