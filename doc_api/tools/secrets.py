"""Mask secrets in anything DocAPI displays, logs or emails.

Only the server applies it; workers and clients handle the real settings and
are not masked. Stored data is never masked: callers mask a copy at the point
where it is displayed or logged.

    mask_secrets_in_struct({"vlm": {"api_key": "sk-..."}})  # {"vlm": {"api_key": "*****"}}
    mask_secrets_in_text("curl -H 'Authorization: Bearer abc...'")
    handler.addFilter(SecretMaskingFilter())  # exceptions a handler writes

Settings come from config: SECRET_MASKING turns masking off,
SECRET_MASKING_ALLOWED_KEYS lists exact key names (case-insensitive, "-" read
as "_") that only look secret, and SECRET_MASKING_HIGH_ENTROPY also masks long
random-looking tokens. Values under allowed keys are still scanned for secret
patterns. Every secret is replaced by SECRET_MASKING_MASK, "*****" by default,
whatever its length.

SECRET_MASKING_PARTIAL_KEYS lists exact key names whose values are revealed
partly, e.g. sk-proj-*****wxyz, so that a user can tell which API key a job
used. SECRET_MASKING_PARTIAL_START and SECRET_MASKING_PARTIAL_END set how many
characters stay visible. These keys override everything else: they are masked
this way even when they do not look secret and even when SECRET_MASKING is
off. A value is revealed only if at least half of it stays hidden; shorter
values are masked fully.

Masking errs on the side of hiding: a masked non-secret is a small loss, a
leaked key is not.
"""
import functools
import logging
import re
from typing import Any, Iterable, Optional

from doc_api.config import config

# A key whose lowercased name contains one of these is secret, at any depth.
SECRET_KEY_PARTS = ("key", "secret", "token", "password", "passwd", "credential",
                    "auth", "cookie", "session", "private")

_SECRET_KEY = re.compile("|".join(SECRET_KEY_PARTS))


def _replacement() -> str:
    """The configured replacement for a secret, SECRET_MASKING_MASK."""
    return config.SECRET_MASKING_MASK


def _normalize_keys(keys: Iterable[str]) -> frozenset:
    return frozenset(key.strip().lower().replace("-", "_") for key in keys if key.strip())


def _partial_keys() -> frozenset:
    return _normalize_keys(config.SECRET_MASKING_PARTIAL_KEYS)


def _is_partial_key(name: Any, partial_keys: frozenset) -> bool:
    return isinstance(name, str) and name.lower().replace("-", "_") in partial_keys


def reveal_partly(value: str) -> str:
    """Keep the configured start and end of value and mask the rest.

    Values too short to keep at least half hidden are masked fully.
    """
    start, end = config.SECRET_MASKING_PARTIAL_START, config.SECRET_MASKING_PARTIAL_END
    if len(value) < 2 * (start + end) or start + end == 0:
        return _replacement()
    return value[:start] + _replacement() + (value[-end:] if end else "")


def _settings(allowed_keys: Optional[Iterable[str]], mask_high_entropy: Optional[bool]):
    """Explicit arguments, else the configured settings."""
    if allowed_keys is None:
        allowed_keys = config.SECRET_MASKING_ALLOWED_KEYS
    if mask_high_entropy is None:
        mask_high_entropy = config.SECRET_MASKING_HIGH_ENTROPY
    return _normalize_keys(allowed_keys), bool(mask_high_entropy)


def is_secret_key(name: Any, allowed_keys: Optional[Iterable[str]] = None) -> bool:
    if not isinstance(name, str):
        return False
    allowed_keys, _ = _settings(allowed_keys, None)
    normalized = name.lower().replace("-", "_")
    return normalized not in allowed_keys and _SECRET_KEY.search(normalized) is not None


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


@functools.lru_cache(maxsize=None)
def _docapi_key_pattern(prefix: str) -> re.Pattern:
    """DocAPI keys, {KEY_PREFIX}.kid.secret: the key id stays visible."""
    return re.compile(rf"(?<![\w.-]){re.escape(prefix)}\.[A-Za-z0-9_-]+\.(?P<secret>[A-Za-z0-9_-]+)")

# key=value, key: value, "key": "value" and 'key': 'value' inside text such as
# logs, tracebacks, reprs and query strings. Group "key" is checked with
# is_secret_key, so the allowlist applies here too.
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


def _mask_group(match: re.Match) -> str:
    start, end = match.span("secret")
    text = match.group(0)
    offset = match.start()
    return text[:start - offset] + _replacement() + text[end - offset:]


def _mask_key_value(match: re.Match, allowed_keys: frozenset) -> str:
    if not is_secret_key(match.group("key"), allowed_keys):
        # The value may hold a pair itself, e.g. "ValueError: password=x".
        prefix = f'{match.group("key")}{match.group("kq")}{match.group("sep")}'
        rest = match.group(0)[len(prefix):]
        return prefix + _KEY_VALUE.sub(lambda m: _mask_key_value(m, allowed_keys), rest)
    if match.group("eq") is not None:
        value = f'\\{match.group("eq")}{_replacement()}\\{match.group("eq")}'
    elif match.group("q") is not None:
        value = f'{match.group("q")}{_replacement()}{match.group("q")}'
    else:
        bare = match.group("bare")
        # Keep nulls and an already masked value as they are.
        if bare.lower() in {"none", "null", _replacement().lower()}:
            return match.group(0)
        value = _replacement()
    return f'{match.group("key")}{match.group("kq")}{match.group("sep")}{value}'


def _reveal_key_value_partly(match: re.Match, partial_keys: frozenset) -> Optional[str]:
    """The pair with its value revealed partly, or None for any other key."""
    if not _is_partial_key(match.group("key"), partial_keys):
        return None
    prefix = f'{match.group("key")}{match.group("kq")}{match.group("sep")}'
    if match.group("eq") is not None:
        quote = f'\\{match.group("eq")}'
        return f'{prefix}{quote}{reveal_partly(match.group(0)[len(prefix) + 2:-2])}{quote}'
    if match.group("q") is not None:
        quote = match.group("q")
        return f'{prefix}{quote}{reveal_partly(match.group(0)[len(prefix) + 1:-1])}{quote}'
    bare = match.group("bare")
    if bare.lower() in {"none", "null", _replacement().lower()}:
        return match.group(0)
    scheme = re.match(r"(?i)(?:Bearer|Basic|Token)\s+", bare)
    scheme = scheme.group(0) if scheme else ""
    return f'{prefix}{scheme}{reveal_partly(bare[len(scheme):])}'


def _protect_partial_pairs(text: str, partial_keys: frozenset, protected: list) -> str:
    """Reveal partial-key pairs partly and swap them for placeholders.

    The placeholders keep the general masking from masking them again.
    """
    def protect(match: re.Match) -> str:
        revealed = _reveal_key_value_partly(match, partial_keys)
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


def _mask_high_entropy_token(match: re.Match) -> str:
    token = match.group(0)
    if re.search(r"[A-Z]", token) and re.search(r"[a-z]", token) and re.search(r"[0-9]", token):
        return _replacement()
    return token


def mask_secrets_in_text(text: str, *, allowed_keys: Optional[Iterable[str]] = None,
                         mask_high_entropy: Optional[bool] = None) -> str:
    """Mask secret patterns in free text, e.g. a log line or a traceback.

    Values of partial keys in key=value pairs are revealed partly, also when
    SECRET_MASKING is off. allowed_keys and mask_high_entropy default to the
    configured settings.
    """
    partial_keys = _partial_keys()
    if not text or not (config.SECRET_MASKING or partial_keys):
        return text
    protected = []
    if partial_keys:
        text = _protect_partial_pairs(text, partial_keys, protected)
    if config.SECRET_MASKING:
        allowed_keys, mask_high_entropy = _settings(allowed_keys, mask_high_entropy)
        for pattern in (*_VALUE_PATTERNS, _docapi_key_pattern(config.KEY_PREFIX)):
            text = pattern.sub(_mask_group, text)
        text = _KEY_VALUE.sub(lambda m: _mask_key_value(m, allowed_keys), text)
        if mask_high_entropy:
            text = _HIGH_ENTROPY.sub(_mask_high_entropy_token, text)
    return _restore_partial_pairs(text, protected) if protected else text


def mask_secrets_in_struct(value: Any, *, allowed_keys: Optional[Iterable[str]] = None,
                           mask_high_entropy: Optional[bool] = None) -> Any:
    """Return a masked copy of value; value itself is never modified.

    Dicts and lists are walked at any depth. The value of a secret-looking key,
    with its whole subtree, becomes the mask while the key stays visible. Strings,
    including values under innocuous keys, are masked by pattern. Other values
    are returned as they are.

    A string under a partial key is revealed partly and any other value under
    it masked fully, whether the key looks secret or not and also when
    SECRET_MASKING is off. allowed_keys and mask_high_entropy default to the
    configured settings.
    """
    partial_keys = _partial_keys()
    if not (config.SECRET_MASKING or partial_keys):
        return value
    return _mask(value, *_settings(allowed_keys, mask_high_entropy), partial_keys)


def _mask(value: Any, allowed_keys: frozenset, mask_high_entropy: bool, partial_keys: frozenset) -> Any:
    if isinstance(value, str):
        return mask_secrets_in_text(value, allowed_keys=allowed_keys, mask_high_entropy=mask_high_entropy)
    if isinstance(value, dict):
        return {key: _mask_item(key, item, allowed_keys, mask_high_entropy, partial_keys)
                for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return type(value)(_mask(item, allowed_keys, mask_high_entropy, partial_keys) for item in value)
    return value


def _mask_item(key: Any, item: Any, allowed_keys: frozenset, mask_high_entropy: bool,
               partial_keys: frozenset) -> Any:
    if item is None:
        return None
    if _is_partial_key(key, partial_keys):
        return reveal_partly(item) if isinstance(item, str) else _replacement()
    if config.SECRET_MASKING and is_secret_key(key, allowed_keys):
        return _replacement()
    return _mask(item, allowed_keys, mask_high_entropy, partial_keys)


# --- Logging ------------------------------------------------------------------

class SecretMaskingFilter(logging.Filter):
    """Mask the message and traceback of exception records; never drops one.

    DocAPI never logs secrets itself. What it cannot avoid is an exception
    echoing what a client sent, e.g. a parsing or database error quoting the
    request. Only records carrying an exception are therefore masked; all
    other records pass unchanged. Attach it to a handler to cover records
    propagated from third-party loggers too; the server attaches it to its
    handlers in LOGGING_CONFIG. Settings default to the configured ones.
    """

    def __init__(self, name: str = "", *, allowed_keys: Optional[Iterable[str]] = None,
                 mask_high_entropy: Optional[bool] = None):
        super().__init__(name)
        self.allowed_keys = None if allowed_keys is None else _normalize_keys(allowed_keys)
        self.mask_high_entropy = mask_high_entropy

    def filter(self, record: logging.LogRecord) -> bool:
        mask_record(record, allowed_keys=self.allowed_keys, mask_high_entropy=self.mask_high_entropy)
        return True


_formatter = logging.Formatter()


def mask_record(record: logging.LogRecord, *, allowed_keys: Optional[Iterable[str]] = None,
                mask_high_entropy: Optional[bool] = None) -> logging.LogRecord:
    if not (config.SECRET_MASKING or config.SECRET_MASKING_PARTIAL_KEYS) \
            or getattr(record, "_secrets_masked", False):
        return record
    if not record.exc_info and not record.exc_text:
        return record
    try:
        message = record.getMessage()
    except Exception:
        # Leave a malformed record to logging's own error reporting.
        return record
    settings = dict(allowed_keys=allowed_keys, mask_high_entropy=mask_high_entropy)
    record.msg = mask_secrets_in_text(message, **settings)
    record.args = None
    if record.exc_info and not record.exc_text:
        # Formatters reuse exc_text instead of formatting exc_info again.
        record.exc_text = _formatter.formatException(record.exc_info)
    if record.exc_text:
        record.exc_text = mask_secrets_in_text(record.exc_text, **settings)
    if record.stack_info:
        record.stack_info = mask_secrets_in_text(record.stack_info, **settings)
    record._secrets_masked = True
    return record
