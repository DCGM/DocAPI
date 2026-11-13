import json
from ipaddress import ip_address
import os
import logging
import time
import socket
import math
from string import Formatter
from typing import Optional
from pathlib import Path
import re
import subprocess

from doc_api.tools.mail.mail_sender import MailSender
from doc_api.tools.secrets import MaskingSettings

COMMIT_PATTERN = re.compile(r"[0-9a-fA-F]{7,64}\Z")


def resolve_git_commit() -> str | None:
    """Honor build metadata, otherwise resolve this source checkout once at startup."""
    commit = os.getenv("APP_GIT_COMMIT", "").strip()
    if commit:
        if not COMMIT_PATTERN.fullmatch(commit):
            raise ValueError("APP_GIT_COMMIT must be a hexadecimal Git commit hash (7–64 characters)")
        return commit

    root = Path(__file__).resolve().parents[1]
    if not (root / ".git").exists():
        return None
    try:
        result = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                                check=True, capture_output=True, text=True, timeout=2)
    except (OSError, subprocess.SubprocessError):
        return None
    commit = result.stdout.strip()
    if not COMMIT_PATTERN.fullmatch(commit):
        return None
    os.environ["APP_GIT_COMMIT"] = commit
    return commit


class DocAPIFormatter(logging.Formatter):
    converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        record.hostname = socket.gethostname()
        return super().format(record)

TRUE_VALUES = {"true", "1"}

MAIL_SERVER_SUBJECT_EXAMPLES = {"server_name": "DocAPI", "admin_server_name": "DocAPI admin"}

JOB_MAIL_SUBJECT_EXAMPLES = {
    **MAIL_SERVER_SUBJECT_EXAMPLES, "status": "DONE", "job_id": "00000000-0000-0000-0000-000000000001",
    "job_id_short": "00000000", "image_count": 10, "attempt": 1,
    "engine_name": "ocr", "engine_version": "1.0", "owner_label": "user",
}

def validate_mail_subject(template: str, setting: str, examples: dict):
    """Reject unsupported placeholders and invalid subjects at startup."""
    try:
        for _, name, _, _ in Formatter().parse(template):
            if name is not None and name not in examples:
                raise ValueError(f"Unknown placeholder {name!r}")
        subject = template.format(**examples)
        if not subject.strip() or "\r" in subject or "\n" in subject:
            raise ValueError("Subject must be a non-empty single line")
    except (KeyError, ValueError, IndexError, TypeError) as error:
        raise ValueError(f"Invalid {setting}: {error}") from error

class Config:
    def __init__(self):
        # =============================================================================
        # Database
        # =============================================================================

        self.DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/doc_api_db")
        # Guards check compatibility; update flags control automatic changes.
        self.DATABASE_REQUIRE_MATCHING_SCHEMA = self._env_bool("DATABASE_REQUIRE_MATCHING_SCHEMA", True)
        self.DATABASE_ALLOW_SCHEMA_UPDATE = self._env_bool("DATABASE_ALLOW_SCHEMA_UPDATE", False)
        self.DATABASE_REQUIRE_MATCHING_COMMIT = self._env_bool("DATABASE_REQUIRE_MATCHING_COMMIT", False)
        self.DATABASE_ALLOW_COMMIT_UPDATE = self._env_bool("DATABASE_ALLOW_COMMIT_UPDATE", True)

        # Connection limits apply per API process.
        self.DATABASE_POOL_SIZE = self._env_int("DATABASE_POOL_SIZE", 10, minimum=1)
        self.DATABASE_MAX_OVERFLOW = self._env_int("DATABASE_MAX_OVERFLOW", 10)
        self.DATABASE_POOL_TIMEOUT_SECONDS = float(os.getenv("DATABASE_POOL_TIMEOUT_SECONDS", "10"))
        if not math.isfinite(self.DATABASE_POOL_TIMEOUT_SECONDS) or self.DATABASE_POOL_TIMEOUT_SECONDS <= 0:
            raise ValueError("DATABASE_POOL_TIMEOUT_SECONDS must be a finite, positive number")

        # =============================================================================
        # HTTP connections
        # =============================================================================

        self.APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
        self.APP_PORT = int(os.getenv("APP_PORT", "9999"))
        # Keep idle HTTP connections longer than the default worker polling interval.
        self.HTTP_KEEP_ALIVE_TIMEOUT_SECONDS = self._env_int("HTTP_KEEP_ALIVE_TIMEOUT_SECONDS", 60)
        # Zero disables the concurrency limit / shutdown deadline.
        self.HTTP_CONCURRENCY_LIMIT = self._env_int("HTTP_CONCURRENCY_LIMIT", 64)
        self.HTTP_BACKLOG = self._env_int("HTTP_BACKLOG", 64, minimum=1)
        self.HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = self._env_int("HTTP_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", 30)

        # =============================================================================
        # Application
        # =============================================================================

        # Data directories default to subdirectories of BASE_DIR.
        self.BASE_DIR = os.getenv("BASE_DIR", "./doc_api_data")
        self.JOBS_DIR = os.getenv("JOBS_DIR", os.path.join(self.BASE_DIR, "jobs"))
        self.RESULTS_DIR = os.getenv("RESULTS_DIR", os.path.join(self.BASE_DIR, "results"))
        self.ARTIFACTS_DIR = os.getenv("ARTIFACTS_DIR", os.path.join(self.BASE_DIR, "artifacts"))
        self.ENGINES_DIR = os.getenv("ENGINES_DIR", os.path.join(self.BASE_DIR, "engines"))

        self.PRODUCTION = self._env_bool("PRODUCTION", False)
        # URL prefix, e.g. /subpath; empty for the root URL.
        self.APP_URL_ROOT = os.getenv("APP_URL_ROOT", "")
        self.APP_BASE_URL = os.getenv("APP_BASE_URL", os.path.join(f"http://{self.APP_HOST}:{self.APP_PORT}", self.APP_URL_ROOT))

        # Admin email identity.
        self.ADMIN_SERVER_NAME = os.getenv("ADMIN_SERVER_NAME", "pc-doc-api-01")

        # Authentication --------------------------------------------------------------

        # API key format: {KEY_PREFIX}.{KEY_ID}.secret
        self.KEY_PREFIX = os.getenv("KEY_PREFIX", "doc-api")
        # Replace both secrets in production.
        self.ADMIN_KEY = os.getenv("ADMIN_KEY", f"{self.KEY_PREFIX}.adminkeykid.changemeadminkeysecret12345")
        self.HMAC_SECRET = os.getenv("HMAC_SECRET", "changemehmacsecret12345")

        # Public identity in the UI, authentication responses and emails.
        self.SERVER_NAME = os.getenv("SERVER_NAME", "DocAPI")
        self.CONTACT_TO_GET_NEW_KEY = os.getenv("CONTACT_TO_GET_NEW_KEY", "admin@pc-doc-api-01.cz")
        self.APP_VERSION = os.getenv("APP_VERSION", "custom")
        # Optional repositories shown in the API documentation.
        self.API_REPOSITORY_URL = os.getenv("API_REPOSITORY_URL", "").strip().rstrip("/")
        self.WORKER_REPOSITORY_URL = os.getenv("WORKER_REPOSITORY_URL", "").strip().rstrip("/")
        # Build metadata in Docker; resolved from this checkout for local Python runs.
        self.APP_GIT_COMMIT = resolve_git_commit()

        # =============================================================================
        # Monitoring
        # =============================================================================

        self.MONITOR_REFRESH_INTERVAL_SECONDS = self._env_int("MONITOR_REFRESH_INTERVAL_SECONDS", 5, minimum=1)
        # PROCESSING assignments always mean active; this threshold applies without a job.
        self.MONITOR_WORKER_OFFLINE_AFTER_SECONDS = self._env_int("MONITOR_WORKER_OFFLINE_AFTER_SECONDS", 60, minimum=1)
        # Display-only deployment identity; empty values retain process detection.
        self.MONITOR_HOSTNAME = os.getenv("MONITOR_HOSTNAME", "").strip()
        self.MONITOR_IP_ADDRESSES = [
            address.strip() for address in os.getenv("MONITOR_IP_ADDRESSES", "").split(",") if address.strip()
        ]
        for address in self.MONITOR_IP_ADDRESSES:
            try:
                ip_address(address)
            except ValueError as error:
                raise ValueError("MONITOR_IP_ADDRESSES must contain comma-separated IPv4 or IPv6 addresses") from error

        # =============================================================================
        # Job processing
        # =============================================================================

        # Checker intervals in seconds (positive integers).
        self.JOB_READINESS_CHECK_INTERVAL_SECONDS = int(os.getenv("JOB_READINESS_CHECK_INTERVAL_SECONDS", "5"))
        if self.JOB_READINESS_CHECK_INTERVAL_SECONDS <= 0:
            raise ValueError("JOB_READINESS_CHECK_INTERVAL_SECONDS must be a positive integer")
        self.JOB_LEASE_CHECK_INTERVAL_SECONDS = int(os.getenv("JOB_LEASE_CHECK_INTERVAL_SECONDS", "5"))
        if self.JOB_LEASE_CHECK_INTERVAL_SECONDS <= 0:
            raise ValueError("JOB_LEASE_CHECK_INTERVAL_SECONDS must be a positive integer")
        # Zero disables lease expiration; ERROR jobs still retry.
        # Positive timeouts expire after timeout + grace without worker updates.
        self.JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "0"))
        if self.JOB_TIMEOUT_SECONDS < 0:
            raise ValueError("JOB_TIMEOUT_SECONDS must be a nonnegative integer")
        self.JOB_TIMEOUT_GRACE_SECONDS = int(os.getenv("JOB_TIMEOUT_GRACE_SECONDS", "10"))
        # Total processing attempts; exhausted jobs become FAILED.
        self.JOB_MAX_ATTEMPTS = max(int(os.getenv("JOB_MAX_ATTEMPTS", "3")), 1)

        # Input cleanup ---------------------------------------------------------------

        # DONE/FAILED jobs only; job records are retained.
        self.DELETE_JOB_INPUTS_ON_COMPLETION = self._env_bool("DELETE_JOB_INPUTS_ON_COMPLETION", False)
        # Seconds from finished_date; 0 = immediate. Positive retention requires a date.
        self.INPUT_RETENTION_SECONDS = int(os.getenv("INPUT_RETENTION_SECONDS", "0"))
        if self.INPUT_RETENTION_SECONDS < 0:
            raise ValueError("INPUT_RETENTION_SECONDS must be a nonnegative integer")
        # Also remove image rows; definitions and image_count remain.
        # Hashes, image UUIDs and upload flags are lost.
        self.DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS = self._env_bool("DELETE_JOB_IMAGE_RECORDS_WITH_INPUTS", False)

        # Output cleanup --------------------------------------------------------------

        self.DELETE_JOB_OUTPUTS = self._env_bool("DELETE_JOB_OUTPUTS", False)
        # Seconds from finished_date; 604800 = 7 days, 0 = immediate.
        # Positive retention keeps jobs without finished_date.
        self.OUTPUT_RETENTION_SECONDS = int(os.getenv("OUTPUT_RETENTION_SECONDS", "604800"))
        if self.OUTPUT_RETENTION_SECONDS < 0:
            raise ValueError("OUTPUT_RETENTION_SECONDS must be a nonnegative integer")
        # results, artifacts, or both.
        self.OUTPUT_CLEANUP_TARGETS = {
            target.strip().lower()
            for target in os.getenv("OUTPUT_CLEANUP_TARGETS", "results,artifacts").split(",")
            if target.strip()
        }
        if not self.OUTPUT_CLEANUP_TARGETS or not self.OUTPUT_CLEANUP_TARGETS <= {"results", "artifacts"}:
            raise ValueError("OUTPUT_CLEANUP_TARGETS must contain results, artifacts, or both")

        # Cleanup schedule ------------------------------------------------------------

        # Run at startup, then every N seconds (positive integer).
        self.CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "60"))
        if self.CLEANUP_INTERVAL_SECONDS <= 0:
            raise ValueError("CLEANUP_INTERVAL_SECONDS must be a positive integer")

        # =============================================================================
        # Input and file validation
        # =============================================================================

        # Filename extensions ---------------------------------------------------------

        # A nonempty allowlist overrides the general extension requirement.
        self.REQUIRE_IMAGE_EXTENSION = self._env_bool("REQUIRE_IMAGE_EXTENSION", False)
        self.ALLOWED_IMAGE_EXTENSIONS = {
            "." + extension.strip().lstrip(".").lower()
            for extension in os.getenv("ALLOWED_IMAGE_EXTENSIONS", "").split(",")
            if extension.strip()
        }

        # Uploaded files --------------------------------------------------------------

        # XML must always parse and images must always decode with OpenCV.
        self.RESULT_ZIP_VALIDATION = self._env_bool("RESULT_ZIP_VALIDATION", True)
        self.ARTIFACTS_ZIP_VALIDATION = self._env_bool("ARTIFACTS_ZIP_VALIDATION", True)
        self.ENGINE_FILES_ZIP_VALIDATION = self._env_bool("ENGINE_FILES_ZIP_VALIDATION", True)
        self.ALTO_VALIDATION = {
            "root": self._env_bool("ALTO_VALIDATE_ROOT", True),
            "namespace": self._env_bool("ALTO_VALIDATE_NAMESPACE", False),
            "has_layout": self._env_bool("ALTO_VALIDATE_HAS_LAYOUT", False),
            "has_page": self._env_bool("ALTO_VALIDATE_HAS_PAGE", False),
            "has_text": self._env_bool("ALTO_VALIDATE_HAS_TEXT", False),
        }

        self.PAGE_VALIDATION = {
            "root": self._env_bool("PAGE_VALIDATE_ROOT", True),
            "namespace": self._env_bool("PAGE_VALIDATE_NAMESPACE", False),
            "has_page": self._env_bool("PAGE_VALIDATE_HAS_PAGE", False),
            "has_text": self._env_bool("PAGE_VALIDATE_HAS_TEXT", False),
        }

        # =============================================================================
        # Mailing
        # =============================================================================

        # Shared subject placeholders -------------------------------------------------

        # Job subjects: server_name, admin_server_name, status (DONE/FAILED), job_id,
        # job_id_short, image_count, attempt, engine_name, engine_version, owner_label.
        # Use {field} or Python format specs; omit unused fields.
        # Server-error subjects accept only server_name and admin_server_name.

        # Internal: server errors and admin job reports -------------------------------

        # Server and sender address are required; recipients are comma-separated.
        self.INTERNAL_MAIL_SERVER = os.getenv("INTERNAL_MAIL_SERVER", None)
        self.INTERNAL_MAIL_PORT = int(os.getenv("INTERNAL_MAIL_PORT", 25))
        self.INTERNAL_MAIL_SENDER_NAME = os.getenv("INTERNAL_MAIL_SENDER_NAME", self.ADMIN_SERVER_NAME)
        self.INTERNAL_MAIL_SENDER_MAIL = os.getenv("INTERNAL_MAIL_SENDER_MAIL", None)
        self.INTERNAL_MAIL_PASSWORD = os.getenv("INTERNAL_MAIL_PASSWORD", None)
        self.INTERNAL_MAIL_USERNAME = os.getenv("INTERNAL_MAIL_USERNAME", None)
        self.INTERNAL_MAIL_SECURITY = os.getenv("INTERNAL_MAIL_SECURITY", "auto")
        self.INTERNAL_MAIL_TIMEOUT_SECONDS = float(os.getenv("INTERNAL_MAIL_TIMEOUT_SECONDS", "60"))
        if not math.isfinite(self.INTERNAL_MAIL_TIMEOUT_SECONDS) or self.INTERNAL_MAIL_TIMEOUT_SECONDS <= 0:
            raise ValueError("INTERNAL_MAIL_TIMEOUT_SECONDS must be a finite, positive number")
        self.INTERNAL_MAIL_RECEIVER_MAILS = [
            address.strip() for address in os.getenv("INTERNAL_MAIL_RECEIVER_MAILS", "").split(",")
            if address.strip()
        ]
        self.INTERNAL_MAIL_FLOOD_LEVEL = int(os.getenv("INTERNAL_MAIL_FLOOD_LEVEL", 10))
        self.INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE = os.getenv(
            "INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE", "[{admin_server_name}] INTERNAL SERVER ERROR")
        validate_mail_subject(self.INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE, "INTERNAL_ERROR_MAIL_SUBJECT_TEMPLATE",
                              MAIL_SERVER_SUBJECT_EXAMPLES)
        self.ADMIN_JOB_MAIL_SUBJECT_TEMPLATE = os.getenv(
            "ADMIN_JOB_MAIL_SUBJECT_TEMPLATE", "[{admin_server_name}] {status} job {job_id_short} ({owner_label})")
        validate_mail_subject(self.ADMIN_JOB_MAIL_SUBJECT_TEMPLATE, "ADMIN_JOB_MAIL_SUBJECT_TEMPLATE", JOB_MAIL_SUBJECT_EXAMPLES)

        # External: user job reports ---------------------------------------------------

        # Owners must have email/email_notifications configured on their API key.
        self.EXTERNAL_MAIL_SERVER = os.getenv("EXTERNAL_MAIL_SERVER", None)
        self.EXTERNAL_MAIL_PORT = int(os.getenv("EXTERNAL_MAIL_PORT", 25))
        self.EXTERNAL_MAIL_SENDER_NAME = os.getenv("EXTERNAL_MAIL_SENDER_NAME", self.SERVER_NAME)
        self.EXTERNAL_MAIL_SENDER_MAIL = os.getenv("EXTERNAL_MAIL_SENDER_MAIL", None)
        self.EXTERNAL_MAIL_PASSWORD = os.getenv("EXTERNAL_MAIL_PASSWORD", None)
        self.EXTERNAL_MAIL_USERNAME = os.getenv("EXTERNAL_MAIL_USERNAME", None)
        self.EXTERNAL_MAIL_SECURITY = os.getenv("EXTERNAL_MAIL_SECURITY", "auto")
        self.EXTERNAL_MAIL_TIMEOUT_SECONDS = float(os.getenv("EXTERNAL_MAIL_TIMEOUT_SECONDS", "60"))
        if not math.isfinite(self.EXTERNAL_MAIL_TIMEOUT_SECONDS) or self.EXTERNAL_MAIL_TIMEOUT_SECONDS <= 0:
            raise ValueError("EXTERNAL_MAIL_TIMEOUT_SECONDS must be a finite, positive number")
        self.EXTERNAL_MAIL_FLOOD_LEVEL = int(os.getenv("EXTERNAL_MAIL_FLOOD_LEVEL", 0))
        self.USER_JOB_MAIL_SUBJECT_TEMPLATE = os.getenv(
            "USER_JOB_MAIL_SUBJECT_TEMPLATE", "[{server_name}] {status} job {job_id_short}")
        validate_mail_subject(self.USER_JOB_MAIL_SUBJECT_TEMPLATE, "USER_JOB_MAIL_SUBJECT_TEMPLATE", JOB_MAIL_SUBJECT_EXAMPLES)

        # Job notification policy (user and admin reports) -----------------------------

        # Persist delivery status; retry failed sends with exponential backoff.
        # Server-error emails do not use this policy.
        self.JOB_MAIL_CHECK_INTERVAL_SECONDS = int(os.getenv("JOB_MAIL_CHECK_INTERVAL_SECONDS", "5"))
        self.JOB_MAIL_MAX_ATTEMPTS = int(os.getenv("JOB_MAIL_MAX_ATTEMPTS", "3"))
        self.JOB_MAIL_RETRY_DELAY_SECONDS = int(os.getenv("JOB_MAIL_RETRY_DELAY_SECONDS", "60"))
        for setting in ("JOB_MAIL_CHECK_INTERVAL_SECONDS", "JOB_MAIL_MAX_ATTEMPTS", "JOB_MAIL_RETRY_DELAY_SECONDS"):
            if getattr(self, setting) <= 0:
                raise ValueError(f"{setting} must be a positive integer")

        # =============================================================================
        # Secret masking
        # =============================================================================

        # Engine settings and job logs shown to user/readonly keys, logged
        # exceptions, access-log query strings and the emails chosen below are
        # masked; admin and worker keys and stored data are not. false turns
        # masking off, except for SECRET_MASKING_PARTIAL_KEYS.
        self.SECRET_MASKING = self._env_bool("SECRET_MASKING", True)
        # Keys whose names contain key, secret, token, password, passwd,
        # credential, auth, cookie, session or private are secret. Comma-separated
        # exact key names that only look secret, e.g. api_key_env,max_tokens,author.
        # Case-insensitive; "-" matches "_". Empty by default.
        self.SECRET_MASKING_ALLOWED_KEYS = [
            name.strip() for name in os.getenv("SECRET_MASKING_ALLOWED_KEYS", "").split(",")
            if name.strip()
        ]
        # Also mask long random-looking tokens in text; hashes and ids may be hidden too.
        self.SECRET_MASKING_HIGH_ENTROPY = self._env_bool("SECRET_MASKING_HIGH_ENTROPY", False)
        # Replaces every secret, whatever its length; empty means the default.
        self.SECRET_MASKING_MASK = os.getenv("SECRET_MASKING_MASK") or "*****"
        # Comma-separated exact key names whose values are revealed partly, e.g.
        # sk-proj-*****wxyz, to tell which API key a job used. They override the
        # rest: masked this way even if they do not look secret and even with
        # SECRET_MASKING off. Values shorter than twice START + END are masked fully.
        self.SECRET_MASKING_PARTIAL_KEYS = [
            name.strip() for name in os.getenv("SECRET_MASKING_PARTIAL_KEYS", "").split(",")
            if name.strip()
        ]
        # Characters kept at the start and end of partly revealed values.
        self.SECRET_MASKING_PARTIAL_START = max(int(os.getenv("SECRET_MASKING_PARTIAL_START", "8")), 0)
        self.SECRET_MASKING_PARTIAL_END = max(int(os.getenv("SECRET_MASKING_PARTIAL_END", "4")), 0)
        # Mask emails before sending: internal ones (server errors, admin job
        # reports) and external ones (user job reports). External reports are
        # sent as the worker wrote them by default.
        self.SECRET_MASKING_INTERNAL_MAIL = self._env_bool("SECRET_MASKING_INTERNAL_MAIL", True)
        self.SECRET_MASKING_EXTERNAL_MAIL = self._env_bool("SECRET_MASKING_EXTERNAL_MAIL", False)

        # =============================================================================
        # Logging
        # =============================================================================

        self.LOGGING_FILE_LEVEL = os.getenv("LOGGING_FILE_LEVEL", logging.INFO)
        self.LOGGING_CONSOLE_LEVEL = os.getenv("LOGGING_CONSOLE_LEVEL", logging.INFO)
        self.LOGGING_DIR = os.getenv("LOGGING_DIR", os.path.join(self.BASE_DIR, "logs"))
        self.LOGGING_CONFIG = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'base': {
                    '()': DocAPIFormatter,
                    'format': '%(asctime)s : %(name)s : %(hostname)s : %(levelname)s : %(message)s'
                }
            },
            # Masks the message and traceback of exception records these handlers
            # write, third-party loggers included, and the query string of
            # uvicorn access records; other records are unchanged.
            # Follows the SECRET_MASKING settings in force for each record.
            'filters': {
                'mask_secrets': {
                    '()': 'doc_api.tools.secrets.SecretMaskingFilter',
                    'settings': self.secret_masking,
                }
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': self.LOGGING_CONSOLE_LEVEL,
                    'formatter': 'base',
                    'filters': ['mask_secrets'],
                    'stream': 'ext://sys.stdout'
                },
                'file_log': {
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'level': self.LOGGING_FILE_LEVEL,
                    'when': 'midnight',
                    'utc': True,
                    'formatter': 'base',
                    'filters': ['mask_secrets'],
                    'filename': os.path.join(self.LOGGING_DIR, f'server.log')
                }
            },
            'loggers': {
                'root': {
                    'level': 'DEBUG',
                    'handlers': [
                        'console',
                        'file_log',
                    ]
                },
                'doc_api.exception_logger': {
                    'level': 'DEBUG',
                    'handlers': [
                        'file_log'
                    ]
                },
                'multipart.multipart': {
                    'level': 'INFO'
                }
            }
        }

        # =============================================================================
        # Swagger documentation
        # =============================================================================

        # Visibility only; endpoint access is unchanged.
        self.SHOW_SECTIONS = set([x.strip() for x in os.getenv("SHOW_SECTIONS", "User, Worker, Admin, Monitor").split(',')])

        self.SHOW_GET_ENGINES = self._env_bool("SHOW_GET_ENGINES", True)
        self.SHOW_PUT_IMAGE = self._env_bool("SHOW_PUT_IMAGE", True)
        self.SHOW_PUT_ALTO = self._env_bool("SHOW_PUT_ALTO", True)
        self.SHOW_PUT_PAGE = self._env_bool("SHOW_PUT_PAGE", True)
        self.SHOW_PUT_METADATA = self._env_bool("SHOW_PUT_METADATA", True)

        # Job definition examples -----------------------------------------------------
        self.JOB_DEFINITION_SUMMARY = os.getenv("JOB_DEFINITION_SUMMARY", "Create Job")
        self.JOB_DEFINITION_DESCRIPTION = os.getenv("JOB_DEFINITION_DESCRIPTION",
                                                    "Create a new job with the specified images and options.\n\n"
                                                    "The job definition must include a list of `images`, each with a unique `name` and `order`.\n\n"
                                                    "If ALTO XML, PAGE XML and Meta JSON files are required, the respective flags `alto_required`, "
                                                    "`page_required`, `meta_json_required` must be set to `true`.\n\n"
                                                    "Each image can supply `image_url`, `alto_url`, and `page_url` for direct worker downloads. "
                                                    "A required input needs either a URL or a normal upload; missing inputs keep the job in NEW. "
                                                    "DocAPI stores links without downloading their contents.\n\n"
                                                    "The images must have specified extensions (e.g., `.jpg`, `.png`) in their names.\n\n"
                                                    "Do not use `alto_required` together with `page_required`, unless your processing worker supports both formats.\n\n"
                                                    "Optionally, you can specify `engine_name` to select a specific processing engine. Default engine will be used if not specified.\n\n")
        self.JOB_DEFINITION_EXAMPLES = json.loads(os.getenv("JOB_DEFINITION_EXAMPLES",
                                                            json.dumps({
                                                                "IMAGE job": {
                                                                    "summary": "Default",
                                                                    "description": "Simple job with two images.",
                                                                    "value": {
                                                                        "images": [
                                                                            {
                                                                                "name": "image0.jpg",
                                                                                "order": 0
                                                                            },
                                                                            {
                                                                                "name": "image1.jpg",
                                                                                "order": 1
                                                                            }
                                                                        ],
                                                                        "meta_json_required": False,
                                                                        "alto_required": False,
                                                                        "page_required": False,
                                                                        "engine_name": "Engine A",
                                                                    }}})))

        if "JOB_DEFINITION_EXAMPLES" not in os.environ:
            self.JOB_DEFINITION_EXAMPLES.update({
                "Linked inputs": {
                    "summary": "Image and ALTO supplied by URL",
                    "value": {"images": [{"name": "page.jpg", "order": 0,
                                           "image_url": "https://files.example.org/page.jpg",
                                           "alto_url": "https://files.example.org/page.alto.xml"}],
                              "alto_required": True},
                },
                "Mixed inputs": {
                    "summary": "Linked image with ALTO supplied by normal upload",
                    "value": {"images": [{"name": "page.jpg", "order": 0,
                                           "image_url": "https://files.example.org/page.jpg"}],
                              "alto_required": True},
                },
            })

        # Metadata upload examples ----------------------------------------------------
        self.META_JSON_SUMMARY = os.getenv("META_JSON_SUMMARY", "Upload Meta JSON")
        self.META_JSON_DESCRIPTION = os.getenv("META_JSON_DESCRIPTION",
                                               "Upload the Meta JSON file for a job.")
        self.META_JSON_EXAMPLES = json.loads(os.getenv("META_JSON_EXAMPLES",
                                                       json.dumps({
                                                           "object": {"summary": "JSON object",
                                                                      "value": {"engine": "ocr", "version": 2}},
                                                           "array": {"summary": "JSON array",
                                                                     "value": ["step1", "step2", "step3"]},
                                                           "primitive": {"summary": "Primitive value", "value": True},
                                                       })))

        # Result download examples ----------------------------------------------------
        self.RESULT_SUMMARY = os.getenv("RESULT_SUMMARY", "Download Result")
        self.RESULT_DESCRIPTION = os.getenv("RESULT_DESCRIPTION",
                                            "Download the result ZIP file for a completed job.")
        self.RESULT_EXAMPLE = os.getenv("RESULT_EXAMPLE",
                                        "(binary ZIP file content)")

        # =============================================================================
        # Testing
        # =============================================================================

        # Automatically created credentials when PRODUCTION is false.
        self.TEST_ADMIN_KEY = os.getenv("TEST_ADMIN_KEY", f"{self.KEY_PREFIX}.testadminkid.testadminkey")
        self.TEST_READONLY_KEY = os.getenv("TEST_READONLY_KEY", f"{self.KEY_PREFIX}.testreadonlykid.testreadonlykey")
        self.TEST_USER_KEY = os.getenv("TEST_USER_KEY", f"{self.KEY_PREFIX}.testuserkid.testuserkey")
        self.TEST_WORKER_KEY = os.getenv("TEST_WORKER_KEY", f"{self.KEY_PREFIX}.testworkerkid.testworkerkey")

        self.TEST_ADMIN_KEY_LABEL = os.getenv("TEST_ADMIN_KEY_LABEL", "test-admin")
        self.TEST_READONLY_KEY_LABEL = os.getenv("TEST_READONLY_KEY_LABEL", "test-readonly")
        self.TEST_USER_KEY_LABEL = os.getenv("TEST_USER_KEY_LABEL", "test-user")
        self.TEST_WORKER_KEY_LABEL = os.getenv("TEST_WORKER_KEY_LABEL", "test-worker")

    @staticmethod
    def _env_int(key: str, default: int, *, minimum: int = 0) -> int:
        try:
            value = int(os.getenv(key, str(default)))
        except ValueError as error:
            raise ValueError(f"{key} must be an integer >= {minimum}") from error
        if value < minimum:
            raise ValueError(f"{key} must be an integer >= {minimum}")
        return value

    def mail_sender(self, channel: str) -> Optional[MailSender]:
        """The sender for the INTERNAL or EXTERNAL mail channel; None when the
        channel has no server or sender address. Internal mail goes to
        INTERNAL_MAIL_RECEIVER_MAILS unless the caller names receivers."""
        if channel not in ("INTERNAL", "EXTERNAL"):
            raise ValueError(f"Unknown mail channel: {channel}")
        server = getattr(self, f"{channel}_MAIL_SERVER")
        sender_mail = getattr(self, f"{channel}_MAIL_SENDER_MAIL")
        if not server or not sender_mail:
            return None
        return MailSender(server=server, sender_mail=sender_mail,
                          port=getattr(self, f"{channel}_MAIL_PORT"),
                          receiver_mails=getattr(self, f"{channel}_MAIL_RECEIVER_MAILS", None),
                          sender_name=getattr(self, f"{channel}_MAIL_SENDER_NAME"),
                          username=getattr(self, f"{channel}_MAIL_USERNAME"),
                          password=getattr(self, f"{channel}_MAIL_PASSWORD"),
                          security=getattr(self, f"{channel}_MAIL_SECURITY"),
                          timeout=getattr(self, f"{channel}_MAIL_TIMEOUT_SECONDS"))

    def secret_masking(self) -> MaskingSettings:
        """The masking tool's settings from the SECRET_MASKING_* settings in force."""
        return MaskingSettings(enabled=self.SECRET_MASKING, mask=self.SECRET_MASKING_MASK,
                               allowed_keys=self.SECRET_MASKING_ALLOWED_KEYS,
                               high_entropy=self.SECRET_MASKING_HIGH_ENTROPY,
                               partial_keys=self.SECRET_MASKING_PARTIAL_KEYS,
                               partial_start=self.SECRET_MASKING_PARTIAL_START,
                               partial_end=self.SECRET_MASKING_PARTIAL_END)

    def _env_bool(self, key: str, default: bool = False) -> bool:
        val = os.getenv(key)
        if val is None:
            return default
        return val.strip().lower() in TRUE_VALUES

    def create_dirs(self):
        os.makedirs(self.JOBS_DIR, exist_ok=True)
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        os.makedirs(self.ARTIFACTS_DIR, exist_ok=True)
        os.makedirs(self.ENGINES_DIR, exist_ok=True)
        os.makedirs(self.LOGGING_DIR, exist_ok=True)

config = Config()

