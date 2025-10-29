import json
import os
import logging
import time
import socket

class DocAPIFormatter(logging.Formatter):
    converter = time.gmtime

    def format(self, record: logging.LogRecord) -> str:
        record.hostname = socket.gethostname()
        return super().format(record)


TRUE_VALUES = {"true", "1"}


class Config:
    def __init__(self):
        self.APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
        self.APP_PORT = int(os.getenv("APP_PORT", "9999"))
        self.PRODUCTION = self._env_bool("PRODUCTION", False)

        # if the app is hosted ${APP_BASE_URL}/subpath, set this to "/subpath"
        self.APP_URL_ROOT = os.getenv("APP_URL_ROOT", "")
        # where the app is hosted
        self.APP_BASE_URL = os.getenv("APP_BASE_URL", os.path.join(f"http://{self.APP_HOST}:{self.APP_PORT}", self.APP_URL_ROOT))


        # sender name for emails sent by the app t admins
        self.ADMIN_SERVER_NAME = os.getenv("ADMIN_SERVER_NAME", "pc-doc-api-01")

        # prefix for API keys, the format of the keys is {KEY_PREFIX}.{KEY_ID}.secret
        self.KEY_PREFIX = os.getenv("KEY_PREFIX", "doc-api")

        # THIS MUST BE CHANGED IN PRODUCTION
        self.ADMIN_KEY = os.getenv("ADMIN_KEY", f"{self.KEY_PREFIX}.adminkid.adminkey")
        self.HMAC_SECRET = os.getenv("HMAC_SECRET", "hmacsecret")



        # displayed in the web interface footer
        # return in detail on unsuccessful authentication
        # used for 401 -> headers={"WWW-Authenticate": f'ApiKey realm="{config.SERVER_NAME}"'}
        self.SERVER_NAME = os.getenv("SERVER_NAME", "DocAPI")

        # displayed in the web interface footer
        self.APP_VERSION = os.getenv("APP_VERSION", "custom")

        # return in detail on unsuccessful authentication
        self.CONTACT_TO_GET_NEW_KEY = os.getenv("CONTACT_TO_GET_NEW_KEY", "admin@pc-doc-api-01.cz")

        self.BASE_DIR = os.getenv("BASE_DIR", "./doc_api_data")
        self.JOBS_DIR = os.getenv("JOBS_DIR", os.path.join(self.BASE_DIR, "jobs"))
        self.RESULTS_DIR = os.getenv("RESULTS_DIR", os.path.join(self.BASE_DIR, "results"))

        self.DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/doc_api_db")
        # if True, allow alembic to upgrade existing DB
        self.DATABASE_ALLOW_UPDATE = self._env_bool("DATABASE_ALLOW_UPDATE", False)
        # if True, skip creating DB and alembic upgrade, simply assume the DB is ready
        self.DATABASE_FORCE = self._env_bool("DATABASE_FORCE", False)

        # job processing configuration
        ################################################################################################################
        # if db_job.last_change for JOB in PROCESSING state is not updated for JOB_TIMEOUT_SECONDS
        #     if db_job.previous_attempts < JOB_MAX_ATTEMPTS - 1
        #         - the job is marked as QUEUED
        #     else
        #         - the job is marked as ERROR
        self.JOB_TIMEOUT_SECONDS = int(os.getenv("JOB_TIMEOUT_SECONDS", "300"))
        self.JOB_TIMEOUT_GRACE_SECONDS = int(os.getenv("JOB_TIMEOUT_GRACE_SECONDS", "10"))
        self.JOB_MAX_ATTEMPTS = max(int(os.getenv("JOB_MAX_ATTEMPTS", "3")), 1)

        # validate uploaded files configuration (valid XML and IMAGE decodable by OpenCV is always checked)
        ################################################################################################################
        # Per-check toggles for ALTO & PAGE XML validation (all default to False)
        # Enable by setting env vars to one of TRUE_VALUES: {"true", "1"} (case-insensitive).
        self.RESULT_ZIP_VALIDATION = self._env_bool("RESULT_ZIP_VALIDATION", True)
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

        # EMAILS and NOTIFICATIONS configuration
        ################################################################################################################

        # internal mailing setting for doc_api.internal_mail_logger
        self.INTERNAL_MAIL_SERVER = os.getenv("INTERNAL_MAIL_SERVER", None)
        self.INTERNAL_MAIL_PORT = os.getenv("INTERNAL_MAIL_PORT", 25)
        self.INTERNAL_MAIL_SENDER_NAME = os.getenv("INTERNAL_MAIL_SENDER_NAME", self.ADMIN_SERVER_NAME)
        self.INTERNAL_MAIL_SENDER_MAIL = os.getenv("INTERNAL_MAIL_SENDER_MAIL", None)
        self.INTERNAL_MAIL_PASSWORD = os.getenv("INTERNAL_MAIL_PASSWORD", None)
        if os.getenv("INTERNAL_MAIL_RECEIVER_MAILS") is not None:
            self.INTERNAL_MAIL_RECEIVER_MAILS = [e.strip() for e in
                                                 os.getenv("INTERNAL_MAIL_RECEIVER_MAILS").split(',')]
        else:
            self.INTERNAL_MAIL_RECEIVER_MAILS = ['user@mail.server.cz']
        self.INTERNAL_MAIL_FLOOD_LEVEL = int(os.getenv("INTERNAL_MAIL_FLOOD_LEVEL", 10))

        # external mailing setting for doc_api.external_mail_logger
        self.EXTERNAL_MAIL_SERVER = os.getenv("EXTERNAL_MAIL_SERVER", None)
        self.EXTERNAL_MAIL_PORT = os.getenv("EXTERNAL_MAIL_PORT", 25)
        self.EXTERNAL_MAIL_SENDER_NAME = os.getenv("EXTERNAL_MAIL_SENDER_NAME", self.SERVER_NAME)
        self.EXTERNAL_MAIL_SENDER_MAIL = os.getenv("EXTERNAL_MAIL_SENDER_MAIL", None)
        self.EXTERNAL_MAIL_PASSWORD = os.getenv("EXTERNAL_MAIL_PASSWORD", None)
        self.EXTERNAL_MAIL_FLOOD_LEVEL = int(os.getenv("EXTERNAL_MAIL_FLOOD_LEVEL", 0))

        # LOGGING configuration
        ################################################################################################################
        self.LOGGING_CONSOLE_LEVEL = os.getenv("LOGGING_CONSOLE_LEVEL", logging.INFO)
        self.LOGGING_FILE_LEVEL = os.getenv("LOGGING_FILE_LEVEL", logging.INFO)
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
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': self.LOGGING_CONSOLE_LEVEL,
                    'formatter': 'base',
                    'stream': 'ext://sys.stdout'
                },
                'file_log': {
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'level': self.LOGGING_FILE_LEVEL,
                    'when': 'midnight',
                    'utc': True,
                    'formatter': 'base',
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

        # API documentation configuration
        ################################################################################################################
        self.SHOW_SECTIONS = set([x.strip() for x in os.getenv("SHOW_SECTIONS", "User, Worker, General, Admin").split(',')])

        self.SHOW_PUT_IMAGE = self._env_bool("SHOW_PUT_IMAGE", True)
        self.SHOW_PUT_ALTO = self._env_bool("SHOW_PUT_ALTO", True)
        self.SHOW_PUT_PAGE = self._env_bool("SHOW_PUT_PAGE", True)
        self.SHOW_PUT_METADATA = self._env_bool("SHOW_PUT_METADATA", True)


        # Job definition examples for documentation (validation is strictly for JobDefinition schema)
        self.JOB_DEFINITION_DESCRIPTION = os.getenv("JOB_DEFINITION_DESCRIPTION",
                                                    "Create a new job with the specified images and options.\n\n"
                                                    "The job definition must include a list of `images`, each with a unique `name` and `order`.\n\n"
                                                    "If ALTO XML, PAGE XML and Meta JSON files are required, the respective flags `alto_required`, "
                                                    "`page_required`, `meta_json_required` must be set to `true`.\n\n"
                                                    "The images must have specified extensions (e.g., `.jpg`, `.png`) in their names.\n\n"
                                                    "Do not use `alto_required` together with `page_required`, unless your processing worker supports both formats.")
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
                                                                        "page_required": False
                                                                    }}})))

        # Meta JSON upload examples for documentation (validation is done only for valid JSON structure, not content)
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

        # Result download examples for documentation
        self.RESULT_DESCRIPTION = os.getenv("RESULT_DESCRIPTION",
                                            "Download the result ZIP file for a completed job.")
        self.RESULT_EXAMPLE = os.getenv("RESULT_EXAMPLE",
                                        "(binary ZIP file content)")


        # Testing setup - these keys will be automatically created in the DB for testing purposes if PRODUCTION is False
        # for tests in doc_api/tests/
        ################################################################################################################
        self.TEST_ADMIN_KEY = os.getenv("TEST_ADMIN_KEY", f"{self.KEY_PREFIX}.testadminkid.testadminkey")
        self.TEST_READONLY_KEY = os.getenv("TEST_READONLY_KEY", f"{self.KEY_PREFIX}.testreadonlykid.testreadonlykey")
        self.TEST_USER_KEY = os.getenv("TEST_USER_KEY", f"{self.KEY_PREFIX}.testuserkid.testuserkey")
        self.TEST_WORKER_KEY = os.getenv("TEST_WORKER_KEY", f"{self.KEY_PREFIX}.testworkerkid.testworkerkey")

        self.TEST_ADMIN_KEY_LABEL = os.getenv("TEST_ADMIN_KEY_LABEL", "test-admin")
        self.TEST_READONLY_KEY_LABEL = os.getenv("TEST_READONLY_KEY_LABEL", "test-readonly")
        self.TEST_USER_KEY_LABEL = os.getenv("TEST_USER_KEY_LABEL", "test-user")
        self.TEST_WORKER_KEY_LABEL = os.getenv("TEST_WORKER_KEY_LABEL", "test-worker")

        self.TEST_HTTP_TIMEOUT = os.getenv("TEST_HTTP_TIMEOUT", "30")

    def _env_bool(self, key: str, default: bool = False) -> bool:
        val = os.getenv(key)
        if val is None:
            return default
        return val.strip().lower() in TRUE_VALUES

    def create_dirs(self):
        os.makedirs(self.JOBS_DIR, exist_ok=True)
        os.makedirs(self.RESULTS_DIR, exist_ok=True)
        os.makedirs(self.LOGGING_DIR, exist_ok=True)


config = Config()

