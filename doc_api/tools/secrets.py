"""Mask secrets in text and data before they are displayed, logged or emailed.

A standalone tool: it reads no configuration. Every function takes a
MaskingSettings, the defaults when omitted; the caller decides where the
settings come from. Stored data is never masked: callers mask a copy at the
point where it is displayed or logged.

    mask_secrets_in_struct({"vlm": {"api_key": "sk-..."}})  # {"vlm": {"api_key": "*****"}}
    mask_secrets_in_text("curl -H 'Authorization: Bearer abc...'", MaskingSettings(mask="[hidden]"))
    handler.addFilter(SecretMaskingFilter())  # exceptions and request query strings a handler writes

MaskingSettings.enabled turns masking off. allowed_keys lists exact key names
(case-insensitive, "-" read as "_") that only look secret, and high_entropy
also masks long random-looking tokens. Values under allowed keys are still
scanned for secret patterns. Every secret is replaced by mask, "*****" by
default, whatever its length.

partial_keys lists exact key names whose values are revealed partly, e.g.
sk-proj-*****wxyz, so that a reader can tell which API key was used.
partial_start and partial_end set how many characters stay visible. These keys
override everything else: they are masked this way even when they do not look
secret and even when masking is not enabled. A value is revealed only if at
least half of it stays hidden; shorter values are masked fully.

Masking errs on the side of hiding: a masked non-secret is a small loss, a
leaked key is not.

DocAPI keys ({KEY_PREFIX}.kid.secret) have no pattern of their own. DocAPI
never logs a key; one reaches masked text only with a label, e.g. an X-API-Key
header, an api_key query parameter or an "api_key" field, which the key/value
rules cover. A bare pattern would also mask ordinary dotted names whenever
KEY_PREFIX is a word used elsewhere, such as a package or host name.
"""
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Union

# A key whose lowercased name contains one of these is secret, at any depth.
SECRET_KEY_PARTS = ("key", "secret", "token", "password", "passwd", "credential",
                    "auth", "cookie", "session", "private")

_SECRET_KEY = re.compile("|".join(SECRET_KEY_PARTS))


def _normalize_keys(keys: Iterable[str]) -> frozenset:
    return frozenset(key.strip().lower().replace("-", "_") for key in keys if key.strip())


@dataclass(frozen=True)
class MaskingSettings:
    """How to mask; the defaults mask every secret-looking value with "*****"."""
    enabled: bool = True
    mask: str = "*****"
    allowed_keys: Iterable[str] = frozenset()
    high_entropy: bool = False
    partial_keys: Iterable[str] = frozenset()
    partial_start: int = 8
    partial_end: int = 4

    def __post_init__(self):
        # Key names are compared case-insensitively, with "-" read as "_".
        object.__setattr__(self, "allowed_keys", _normalize_keys(self.allowed_keys))
        object.__setattr__(self, "partial_keys", _normalize_keys(self.partial_keys))
        object.__setattr__(self, "mask", self.mask or "*****")
        object.__setattr__(self, "partial_start", max(int(self.partial_start), 0))
        object.__setattr__(self, "partial_end", max(int(self.partial_end), 0))

    @property
    def active(self) -> bool:
        """Whether anything is masked at all: enabled, or partial keys set."""
        return self.enabled or bool(self.partial_keys)


DEFAULT_SETTINGS = MaskingSettings()


def _is_partial_key(name: Any, settings: MaskingSettings) -> bool:
    return isinstance(name, str) and name.lower().replace("-", "_") in settings.partial_keys


def reveal_partly(value: str, settings: MaskingSettings = DEFAULT_SETTINGS) -> str:
    """Keep the start and end of value set in settings and mask the rest.

    Values too short to keep at least half hidden are masked fully.
    """
    start, end = settings.partial_start, settings.partial_end
    if len(value) < 2 * (start + end) or start + end == 0:
        return settings.mask
    return value[:start] + settings.mask + (value[-end:] if end else "")


def is_secret_key(name: Any, settings: MaskingSettings = DEFAULT_SETTINGS) -> bool:
    """Whether a key's name looks secret and is not allowed in settings."""
    if not isinstance(name, str):
        return False
    normalized = name.lower().replace("-", "_")
    return normalized not in settings.allowed_keys and _SECRET_KEY.search(normalized) is not None


# --- Patterns for free text ---------------------------------------------------

# Each pattern masks its "secret" group and keeps the rest of the match.
_VALUE_PATTERNS = [
    # Credentials in URLs: scheme://user:password@host keeps the user.
    re.compile(r"(?i)\b[a-z][a-z0-9+.-]*://[^\s:/@]*:(?P<secret>[^\s@/]+)@"),
    # Authorization schemes.
    re.compile(r"(?i)\bBearer\s+(?P<secret>[A-Za-z0-9._~+/=-]{8,})"),
    re.compile(r"(?i)\bBasic\s+(?P<secret>[A-Za-z0-9+/=]{12,})"),
    # Known token shapes. sk- also covers sk-or-v1-, sk-proj- and sk-ant-.
    re.compile(r"\b(?P<secret>sk-[A-Za-z0-9_-]{16,})"),
    re.compile(r"\b(?P<secret>gh[pousr]_[A-Za-z0-9]{20,})"),
    re.compile(r"\b(?P<secret>github_pat_[A-Za-z0-9_]{20,})"),
    re.compile(r"\b(?P<secret>glpat-[A-Za-z0-9_-]{20,})"),
    re.compile(r"\b(?P<secret>hf_[A-Za-z0-9]{30,})"),
    re.compile(r"\b(?P<secret>xox[abprs]-[A-Za-z0-9-]{10,})"),
    re.compile(r"\b(?P<secret>AIza[0-9A-Za-z_-]{35})"),
    re.compile(r"\b(?P<secret>(?:AKIA|ASIA)[A-Z0-9]{16})\b"),
    # JWTs: base64url header and payload both start with '{"'.
    re.compile(r"\b(?P<secret>eyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)"),
]


# key=value, key: value, "key": "value" and 'key': 'value' inside text such as
# logs, tracebacks, reprs and query strings. Group "key" is checked with
# is_secret_key, so the allowed keys apply here too.
_KEY_VALUE = re.compile(
    r"""(?P<key>[A-Za-z_][\w.-]*)(?P<kq>\\?["']?)(?P<sep>\s*[:=]\s*)"""
    r"""(?:\\(?P<eq>["'])(?:(?!\\(?P=eq)).)*\\(?P=eq)"""
    r"""|(?P<q>["'])(?:\\.|(?!(?P=q)).)*(?P=q)"""
    r"""|(?P<bare>(?:(?:Bearer|Basic|Token)\s+)?[^\s,;&'"(){}\[\]<>]+))""",
    re.IGNORECASE,
)

# Long tokens of the base64/base64url alphabet mixing upper case, lower case
# and digits. Requiring all three classes leaves hex digests and UUIDs alone.
_HIGH_ENTROPY = re.compile(r"(?<![A-Za-z0-9+/_=-])[A-Za-z0-9+/_-]{32,}={0,2}(?![A-Za-z0-9+/_=-])")


def _mask_group(match: re.Match, settings: MaskingSettings) -> str:
    start, end = match.span("secret")
    text = match.group(0)
    offset = match.start()
    return text[:start - offset] + settings.mask + text[end - offset:]


def _mask_key_value(match: re.Match, settings: MaskingSettings) -> str:
    if not is_secret_key(match.group("key"), settings):
        # The value may hold a pair itself, e.g. "ValueError: password=x".
        prefix = f'{match.group("key")}{match.group("kq")}{match.group("sep")}'
        rest = match.group(0)[len(prefix):]
        return prefix + _KEY_VALUE.sub(lambda m: _mask_key_value(m, settings), rest)
    if match.group("eq") is not None:
        value = f'\\{match.group("eq")}{settings.mask}\\{match.group("eq")}'
    elif match.group("q") is not None:
        value = f'{match.group("q")}{settings.mask}{match.group("q")}'
    else:
        bare = match.group("bare")
        # Keep nulls and an already masked value as they are.
        if bare.lower() in {"none", "null", settings.mask.lower()}:
            return match.group(0)
        value = settings.mask
    return f'{match.group("key")}{match.group("kq")}{match.group("sep")}{value}'


def _reveal_key_value_partly(match: re.Match, settings: MaskingSettings) -> Optional[str]:
    """The pair with its value revealed partly, or None for any other key."""
    if not _is_partial_key(match.group("key"), settings):
        return None
    prefix = f'{match.group("key")}{match.group("kq")}{match.group("sep")}'
    if match.group("eq") is not None:
        quote = f'\\{match.group("eq")}'
        return f'{prefix}{quote}{reveal_partly(match.group(0)[len(prefix) + 2:-2], settings)}{quote}'
    if match.group("q") is not None:
        quote = match.group("q")
        return f'{prefix}{quote}{reveal_partly(match.group(0)[len(prefix) + 1:-1], settings)}{quote}'
    bare = match.group("bare")
    if bare.lower() in {"none", "null", settings.mask.lower()}:
        return match.group(0)
    scheme = re.match(r"(?i)(?:Bearer|Basic|Token)\s+", bare)
    scheme = scheme.group(0) if scheme else ""
    return f'{prefix}{scheme}{reveal_partly(bare[len(scheme):], settings)}'


def _protect_partial_pairs(text: str, settings: MaskingSettings, protected: list) -> str:
    """Reveal partial-key pairs partly and swap them for placeholders.

    The placeholders keep the general masking from masking them again.
    """
    def protect(match: re.Match) -> str:
        revealed = _reveal_key_value_partly(match, settings)
        if revealed is None:
            # The value may hold a pair itself, e.g. "ValueError: api_key=x".
            prefix = f'{match.group("key")}{match.group("kq")}{match.group("sep")}'
            return prefix + _KEY_VALUE.sub(protect, match.group(0)[len(prefix):])
        protected.append(revealed)
        return f"\x00{len(protected) - 1}\x00"
    return _KEY_VALUE.sub(protect, text)


def _restore_partial_pairs(text: str, protected: list) -> str:
    # A placeholder the general masking damaged stays masked: nothing leaks.
    return re.sub(r"\x00(\d+)\x00", lambda m: protected[int(m.group(1))], text)


def _mask_high_entropy_token(match: re.Match, settings: MaskingSettings) -> str:
    token = match.group(0)
    if re.search(r"[A-Z]", token) and re.search(r"[a-z]", token) and re.search(r"[0-9]", token):
        return settings.mask
    return token


def mask_secrets_in_text(text: str, settings: MaskingSettings = DEFAULT_SETTINGS) -> str:
    """Mask secret patterns in free text, e.g. a log line or a traceback.

    Values of partial keys in key=value pairs are revealed partly, also when
    masking is not enabled.
    """
    if not text or not settings.active:
        return text
    protected = []
    if settings.partial_keys:
        text = _protect_partial_pairs(text, settings, protected)
    if settings.enabled:
        for pattern in _VALUE_PATTERNS:
            text = pattern.sub(lambda m: _mask_group(m, settings), text)
        text = _KEY_VALUE.sub(lambda m: _mask_key_value(m, settings), text)
        if settings.high_entropy:
            text = _HIGH_ENTROPY.sub(lambda m: _mask_high_entropy_token(m, settings), text)
    return _restore_partial_pairs(text, protected) if protected else text


def mask_secrets_in_struct(value: Any, settings: MaskingSettings = DEFAULT_SETTINGS) -> Any:
    """Return a masked copy of value; value itself is never modified.

    Dicts and lists are walked at any depth. The value of a secret-looking key,
    with its whole subtree, becomes the mask while the key stays visible. Strings,
    including values under innocuous keys, are masked by pattern. Other values
    are returned as they are.

    A string under a partial key is revealed partly and any other value under
    it masked fully, whether the key looks secret or not and also when masking
    is not enabled.
    """
    if not settings.active:
        return value
    return _mask(value, settings)


def _mask(value: Any, settings: MaskingSettings) -> Any:
    if isinstance(value, str):
        return mask_secrets_in_text(value, settings)
    if isinstance(value, dict):
        return {key: _mask_item(key, item, settings) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_mask(item, settings) for item in value)
    return value


def _mask_item(key: Any, item: Any, settings: MaskingSettings) -> Any:
    if item is None:
        return None
    if _is_partial_key(key, settings):
        return reveal_partly(item, settings) if isinstance(item, str) else settings.mask
    if settings.enabled and is_secret_key(key, settings):
        return settings.mask
    return _mask(item, settings)


# --- Logging ------------------------------------------------------------------

class SecretMaskingFilter(logging.Filter):
    """Mask exception records and access-log query strings; never drops a record.

    DocAPI never logs secrets itself. What it cannot avoid is a record echoing
    what a client sent: an exception quoting the request, e.g. a parsing or
    database error, and the access log line of a request whose query string
    carries a key (?api_key=...). Exception records are masked in full; of an
    access record only the path argument is masked, and only when it has a
    query string. All other records pass unchanged. Attach it to a handler to
    cover records propagated from third-party loggers too.

    settings is a MaskingSettings, or a function returning one that is called
    for each record, so that changed settings apply at once; the defaults when
    omitted.
    """

    def __init__(self, name: str = "", *,
                 settings: Union[MaskingSettings, Callable[[], MaskingSettings]] = DEFAULT_SETTINGS):
        super().__init__(name)
        self.settings = settings

    def filter(self, record: logging.LogRecord) -> bool:
        settings = self.settings() if callable(self.settings) else self.settings
        mask_record(record, settings)
        return True


_formatter = logging.Formatter()

# Uvicorn's access record: '%s - "%s %s HTTP/%s" %d' with (client, method,
# path with query string, HTTP version, status) as its arguments.
ACCESS_LOGGER = "uvicorn.access"
_ACCESS_PATH_ARG = 2


def _mask_access_record(record: logging.LogRecord, settings: MaskingSettings) -> logging.LogRecord:
    """Mask the query string of an access record's path; skip paths without one."""
    args = record.args
    if isinstance(args, tuple) and len(args) > _ACCESS_PATH_ARG:
        path = args[_ACCESS_PATH_ARG]
        if isinstance(path, str) and "?" in path:
            record.args = (*args[:_ACCESS_PATH_ARG], mask_secrets_in_text(path, settings),
                           *args[_ACCESS_PATH_ARG + 1:])
    record._secrets_masked = True
    return record


def mask_record(record: logging.LogRecord, settings: MaskingSettings = DEFAULT_SETTINGS) -> logging.LogRecord:
    if not settings.active or getattr(record, "_secrets_masked", False):
        return record
    if record.name == ACCESS_LOGGER and not record.exc_info:
        return _mask_access_record(record, settings)
    if not record.exc_info and not record.exc_text:
        return record
    try:
        message = record.getMessage()
    except Exception:
        # Leave a malformed record to logging's own error reporting.
        return record
    record.msg = mask_secrets_in_text(message, settings)
    record.args = None
    if record.exc_info and not record.exc_text:
        # Formatters reuse exc_text instead of formatting exc_info again.
        record.exc_text = _formatter.formatException(record.exc_info)
    if record.exc_text:
        record.exc_text = mask_secrets_in_text(record.exc_text, settings)
    if record.stack_info:
        record.stack_info = mask_secrets_in_text(record.stack_info, settings)
    record._secrets_masked = True
    return record
