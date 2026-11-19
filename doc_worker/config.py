"""Settings for a worker built on DocWorkerWrapper, as a starting point.

Plain values read from the environment variable of the same name, with the
defaults the wrapper, its Connector and its MailSender use. A setting without a
sensible default (the API URL and key, the mail server and receivers, ...) is
None or empty, so it is clear what to fill in. An unset or empty variable keeps
the default. doc_worker/.env_example documents every setting.

A Config is built once, from the environment and any overrides, and checked:
Config({"POLLING_INTERVAL": "1"}) reads POLLING_INTERVAL as "1" whatever the
environment says, e.g. from a command-line argument (see
doc_worker/dummy_worker.py). It then describes a worker that can start and is
only read. DocWorkerWrapper.from_config() builds the worker from it, so a
worker needs only its own process_job::

    from doc_worker.config import Config

    MyWorker.from_config(Config()).start()

A worker with settings of its own extends the config, and its constructor takes
each of them as the parameter of the same name in lower case (MODEL_PATH as
model_path); from_config passes them, and fails on a setting without such a
parameter. The settings of Config itself are only for the wrapper::

    class MyConfig(Config):
        def read(self):
            super().read()
            self.MODEL_PATH = self._str("MODEL_PATH", "model.bin")

        def check(self):
            super().check()
            if not self.MODEL_PATH.endswith(".bin"):
                raise ValueError("MODEL_PATH must be a .bin file")

    class MyWorker(DocWorkerWrapper):
        def __init__(self, *args, model_path, **kwargs):
            ...

A worker that builds something from its settings first overrides
subclass_args() and passes its arguments itself; the wrapper's are built as
usual::

    class MyWorker(DocWorkerWrapper):
        def __init__(self, *args, model, **kwargs):
            ...

        @classmethod
        def subclass_args(cls, config):
            return {"model": load_model(config.MODEL_PATH)}

The module reads only the environment, never the DocAPI server configuration.
"""
import math
import os
from typing import Mapping, Optional

TRUE_VALUES = {"true", "1", "yes", "on"}
MAIL_SECURITY_MODES = ("auto", "none", "ssl", "starttls")
FALSE_VALUES = {"false", "0", "no", "off"}


class Config:
    def __init__(self, overrides: Optional[Mapping[str, str]] = None):
        """Read every setting; an override replaces the environment variable of its name.

        overrides maps setting names to text, as in the environment. An empty value
        counts as unset, as an empty variable does: the setting takes its default, so
        a setting without one (e.g. WORKER_KEY_FILE) is cleared. Settings not in
        overrides are read from the environment.
        """
        self._overrides = dict(overrides or {})
        for name, value in self._overrides.items():
            if not isinstance(value, str):
                raise TypeError(f"Override {name} must be text, as in the environment, not {value!r}")
        self.read()
        # Every setting has been read now, so an override that none of them took is not
        # a setting, e.g. a misspelt name.
        unknown = set(self._overrides) - set(vars(self))
        if unknown:
            raise TypeError(f"Unknown settings: {', '.join(sorted(unknown))}")
        del self._overrides
        self.check()

    def read(self):
        """Read the settings, each from its override or else its environment variable.

        A subclass adds its own settings by extending this, and checks them by
        extending check(); both call super() first.
        """
        # Worker
        ################################################################################################################
        # The worker's name; empty takes the label of the worker's API key from DocAPI
        # at startup. Set it only to override that label, e.g. "OCR worker prod". The name
        # appears in the subject and the "worker" line of every email the worker sends, and
        # is sent as the User-Agent of its requests, so the DocAPI proxy logs show which
        # worker made a request.
        self.WORKER_IDENTITY = self._str("WORKER_IDENTITY", None)

        # DocAPI connection
        ################################################################################################################
        # The DocAPI base URL, e.g. https://docapi.example.org. Required.
        self.API_URL = self._str("API_URL", None)
        # The worker's API key, or a file holding it (preferred: the key then never
        # sits in the environment or on a command line). Exactly one is required.
        self.WORKER_KEY = self._str("WORKER_KEY", None)
        self.WORKER_KEY_FILE = self._str("WORKER_KEY_FILE", None)
        # How long a request to DocAPI may wait to connect, and for each read of the
        # response (not the whole transfer).
        self.API_CONNECT_TIMEOUT_SECONDS = self._positive_seconds("API_CONNECT_TIMEOUT_SECONDS", 10)
        self.API_READ_TIMEOUT_SECONDS = self._positive_seconds("API_READ_TIMEOUT_SECONDS", 300)

        # Directories
        ################################################################################################################
        # BASE_DIR holds jobs/ and engines/; JOBS_DIR and ENGINES_DIR override them.
        # Either BASE_DIR, or both JOBS_DIR and ENGINES_DIR, are required.
        self.BASE_DIR = self._str("BASE_DIR", None)
        self.JOBS_DIR = self._str("JOBS_DIR", None)
        self.ENGINES_DIR = self._str("ENGINES_DIR", None)
        # Delete a job's directory once its successful result has been reported to DocAPI,
        # so finished jobs do not fill the disk; failed jobs are kept for inspection.
        self.CLEANUP_JOB_DIR = self._bool("CLEANUP_JOB_DIR", False)
        # Delete the earlier download of an engine when it is downloaded again because its
        # files changed on DocAPI, so only the current copy stays on disk.
        self.CLEANUP_OLD_ENGINES = self._bool("CLEANUP_OLD_ENGINES", False)
        # A JSONL file recording every job attempt on disk; ALIVE emails include its statistics.
        # Empty keeps no history.
        self.JOB_HISTORY_PATH = self._str("JOB_HISTORY_PATH", None)

        # Polling and inputs
        ################################################################################################################
        # Seconds to wait before asking DocAPI for a job again after it had none; at least 1,
        # so an idle worker does not flood DocAPI with requests.
        self.POLLING_INTERVAL = self._number("POLLING_INTERVAL", 5)
        # Prefer normal API uploads when both sources exist, unless enabled.
        self.PREFER_IMAGE_URL = self._bool("PREFER_IMAGE_URL", False)
        self.PREFER_ALTO_URL = self._bool("PREFER_ALTO_URL", False)
        self.PREFER_PAGE_URL = self._bool("PREFER_PAGE_URL", False)

        # Worker mail
        ################################################################################################################
        # The worker emails its own state: failures (FAILURE), DocAPI outages (DISCONNECTED,
        # STILL DISCONNECTED, RECONNECTED) and regular status reports (ALIVE, see below).
        # Without MAIL_SERVER it sends no emails, and the other mail settings must stay empty;
        # with it, MAIL_SENDER_MAIL and MAIL_RECEIVER_MAILS are required. Emails about jobs come
        # from DocAPI, not the worker.
        # Connection: where and how to connect.
        self.MAIL_SERVER = self._str("MAIL_SERVER", None)
        self.MAIL_PORT = self._nonnegative_int("MAIL_PORT", 25)
        # auto (ssl with a password, none without), none, ssl or starttls
        self.MAIL_SECURITY = self._str("MAIL_SECURITY", "auto")
        # PEM file with the CAs that verify the server's TLS certificate; empty uses the system's
        self.MAIL_CA_FILE = self._str("MAIL_CA_FILE", None)
        self.MAIL_TIMEOUT_SECONDS = self._positive_seconds("MAIL_TIMEOUT_SECONDS", 60)
        # Sender: who the emails are from. MAIL_SENDER_MAIL is required with MAIL_SERVER.
        self.MAIL_SENDER_MAIL = self._str("MAIL_SENDER_MAIL", None)
        self.MAIL_SENDER_NAME = self._str("MAIL_SENDER_NAME", None)
        # Login: used only with a password; the login name is MAIL_SENDER_MAIL by default.
        self.MAIL_USERNAME = self._str("MAIL_USERNAME", None)
        self.MAIL_PASSWORD = self._str("MAIL_PASSWORD", None)
        # Receivers: who gets the worker's emails, comma separated; each gets a separate email.
        self.MAIL_RECEIVER_MAILS = self._list("MAIL_RECEIVER_MAILS")
        # Limits: how many emails may go out per sliding window; 0 turns a limit off.
        # At most this many emails with one subject to one receiver per window.
        self.MAIL_SEND_RECEIVER_LIMIT = self._nonnegative_int("MAIL_SEND_RECEIVER_LIMIT", 10)
        self.MAIL_SEND_RECEIVER_WINDOW_SECONDS = self._positive_seconds("MAIL_SEND_RECEIVER_WINDOW_SECONDS", 3600)
        # At most this many emails in total per window, to all receivers together.
        self.MAIL_SEND_SERVER_LIMIT = self._nonnegative_int("MAIL_SEND_SERVER_LIMIT", 100)
        self.MAIL_SEND_SERVER_WINDOW_SECONDS = self._positive_seconds("MAIL_SEND_SERVER_WINDOW_SECONDS", 3600)
        # The host in the worker's emails; empty uses socket.gethostname().
        self.WORKER_HOSTNAME = self._str("WORKER_HOSTNAME", None)
        # The subject of every email the worker sends, a Python format string with {identity},
        # {hostname}, {time} (UTC datetime, e.g. {time:%Y-%m-%d}) and {status}, the kind of email:
        # FAILURE when the worker fails, DISCONNECTED, STILL DISCONNECTED or
        # RECONNECTED for DocAPI outages and ALIVE for the regular status reports (see their sections below).
        self.MAIL_SUBJECT = self._str("MAIL_SUBJECT", "[{identity}] {status} ({hostname})")

        # DocAPI outages
        ################################################################################################################
        # Emails with the subject statuses DISCONNECTED, STILL DISCONNECTED and RECONNECTED,
        # sent when mail is set up and DocAPI cannot be reached.
        # DISCONNECTED once DocAPI has been unreachable this long while the worker is idle;
        # an outage during a job is mailed at once.
        self.OUTAGE_GRACE_SECONDS = self._nonnegative_seconds("OUTAGE_GRACE_SECONDS", 60)
        # A STILL DISCONNECTED reminder this often while DocAPI stays unreachable; RECONNECTED
        # follows when it is back.
        self.OUTAGE_MAIL_INTERVAL_SECONDS = self._positive_seconds("OUTAGE_MAIL_INTERVAL_SECONDS", 3600)

        # ALIVE mail
        ################################################################################################################
        # Emails with the subject status ALIVE, sent when mail is set up (MAIL_SERVER and
        # MAIL_RECEIVER_MAILS): one at startup, then one every interval, with the worker's
        # uptime, its last and active job and, with JOB_HISTORY_PATH, its job statistics.
        # Send an ALIVE email when the worker starts.
        self.ALIVE_MAIL_STARTUP = self._bool("ALIVE_MAIL_STARTUP", True)
        # Seconds between the ALIVE emails; the default is one day (86400), 0 sends none.
        self.ALIVE_MAIL_INTERVAL_SECONDS = self._nonnegative_seconds("ALIVE_MAIL_INTERVAL_SECONDS", 86400)

        # Error messages
        ################################################################################################################
        # The texts a job gets when the worker fails at one of its steps: _LOG is the technical
        # report for admins (diagnostics are appended to it), _LOG_USER the report for the job's
        # owner. Empty keeps the text shown. ERROR_DOWNLOAD_INPUT_* may use {kind} (image, ALTO
        # or PAGE) and {filename}.
        self.ERROR_REQUEST_JOB_LOG = self._str("ERROR_REQUEST_JOB_LOG", "Failed to request job")
        self.ERROR_REQUEST_JOB_LOG_USER = self._str("ERROR_REQUEST_JOB_LOG_USER", "Failed to request job")
        self.ERROR_REQUEST_JOB_LEASE_LOG = self._str("ERROR_REQUEST_JOB_LEASE_LOG", "Failed to request job lease")
        self.ERROR_REQUEST_JOB_LEASE_LOG_USER = self._str("ERROR_REQUEST_JOB_LEASE_LOG_USER",
                                                          "Failed to request job lease")
        self.ERROR_GET_JOB_DETAILS_LOG = self._str("ERROR_GET_JOB_DETAILS_LOG", "Failed to get job details")
        self.ERROR_GET_JOB_DETAILS_LOG_USER = self._str("ERROR_GET_JOB_DETAILS_LOG_USER", "Failed to get job details")
        self.ERROR_NO_CURRENT_JOB_LOG = self._str("ERROR_NO_CURRENT_JOB_LOG", "No current job set")
        self.ERROR_NO_CURRENT_JOB_LOG_USER = self._str("ERROR_NO_CURRENT_JOB_LOG_USER", "No current job set")
        self.ERROR_PREPARE_ENGINE_FILES_LOG = self._str("ERROR_PREPARE_ENGINE_FILES_LOG",
                                                        "Failed to download or prepare engine files")
        self.ERROR_PREPARE_ENGINE_FILES_LOG_USER = self._str("ERROR_PREPARE_ENGINE_FILES_LOG_USER",
                                                             "Failed to download or prepare engine files")
        self.ERROR_DOWNLOAD_ENGINE_FILES_LOG = self._str("ERROR_DOWNLOAD_ENGINE_FILES_LOG",
                                                         "Failed to download engine files")
        self.ERROR_DOWNLOAD_ENGINE_FILES_LOG_USER = self._str("ERROR_DOWNLOAD_ENGINE_FILES_LOG_USER",
                                                              "Failed to download engine files")
        self.ERROR_DOWNLOAD_JOB_DATA_LOG = self._str("ERROR_DOWNLOAD_JOB_DATA_LOG",
                                                     "Failed to download job data from server")
        self.ERROR_DOWNLOAD_JOB_DATA_LOG_USER = self._str("ERROR_DOWNLOAD_JOB_DATA_LOG_USER",
                                                          "Failed to download job data from server")
        self.ERROR_DOWNLOAD_INPUT_LOG = self._str("ERROR_DOWNLOAD_INPUT_LOG",
                                                  "Failed to download {kind} for {filename}")
        self.ERROR_DOWNLOAD_INPUT_LOG_USER = self._str("ERROR_DOWNLOAD_INPUT_LOG_USER",
                                                       "Failed to download {kind} for {filename}")
        self.ERROR_DOWNLOAD_META_JSON_LOG = self._str("ERROR_DOWNLOAD_META_JSON_LOG", "Failed to download meta JSON")
        self.ERROR_DOWNLOAD_META_JSON_LOG_USER = self._str("ERROR_DOWNLOAD_META_JSON_LOG_USER",
                                                           "Failed to download meta JSON")
        self.ERROR_CREATE_JOB_WORKSPACE_LOG = self._str("ERROR_CREATE_JOB_WORKSPACE_LOG",
                                                        "Unable to create job workspace directory")
        self.ERROR_CREATE_JOB_WORKSPACE_LOG_USER = self._str("ERROR_CREATE_JOB_WORKSPACE_LOG_USER",
                                                             "Unable to create job workspace directory")
        self.ERROR_JOB_DATA_PATH_LOG = self._str("ERROR_JOB_DATA_PATH_LOG", "Failed to get job data path")
        self.ERROR_JOB_DATA_PATH_LOG_USER = self._str("ERROR_JOB_DATA_PATH_LOG_USER", "Failed to get job data path")
        self.ERROR_PROCESS_JOB_LOG = self._str("ERROR_PROCESS_JOB_LOG", "Processing failed due to an internal error")
        self.ERROR_PROCESS_JOB_LOG_USER = self._str("ERROR_PROCESS_JOB_LOG_USER",
                                                    "Processing failed due to an internal error")
        self.ERROR_PACKAGE_RESULTS_LOG = self._str("ERROR_PACKAGE_RESULTS_LOG", "Failed to package processing results")
        self.ERROR_PACKAGE_RESULTS_LOG_USER = self._str("ERROR_PACKAGE_RESULTS_LOG_USER",
                                                        "Failed to package processing results")
        self.ERROR_NO_JOB_FOR_RESULT_UPLOAD_LOG = self._str("ERROR_NO_JOB_FOR_RESULT_UPLOAD_LOG",
                                                            "No current job to upload results for")
        self.ERROR_NO_JOB_FOR_RESULT_UPLOAD_LOG_USER = self._str("ERROR_NO_JOB_FOR_RESULT_UPLOAD_LOG_USER",
                                                                 "No current job to upload results for")
        self.ERROR_UPLOAD_RESULTS_LOG = self._str("ERROR_UPLOAD_RESULTS_LOG", "Failed to upload results")
        self.ERROR_UPLOAD_RESULTS_LOG_USER = self._str("ERROR_UPLOAD_RESULTS_LOG_USER", "Failed to upload results")
        self.ERROR_UPLOAD_RESULTS_TO_SERVER_LOG = self._str("ERROR_UPLOAD_RESULTS_TO_SERVER_LOG",
                                                            "Failed to upload results to server")
        self.ERROR_UPLOAD_RESULTS_TO_SERVER_LOG_USER = self._str("ERROR_UPLOAD_RESULTS_TO_SERVER_LOG_USER",
                                                                 "Failed to upload results to server")
        self.ERROR_UNEXPECTED_PROCESSING_LOG = self._str("ERROR_UNEXPECTED_PROCESSING_LOG",
                                                         "An unexpected error occurred during processing")
        self.ERROR_UNEXPECTED_PROCESSING_LOG_USER = self._str("ERROR_UNEXPECTED_PROCESSING_LOG_USER",
                                                              "An unexpected error occurred during processing")

    @classmethod
    def setting_names(cls) -> tuple:
        """The names of the settings this class reads, in order.

        read() runs on an empty probe, so every setting takes its default and
        neither the environment nor any check is involved.
        """
        probe = cls.__new__(cls)
        probe._raw = lambda key: ""
        probe.read()
        return tuple(name for name in vars(probe) if name.isupper())

    def check(self):
        """Reject settings that are missing or cannot work together."""
        if not self.API_URL:
            raise ValueError("API_URL is required")
        if not self.API_URL.startswith(("http://", "https://")):
            raise ValueError(f"API_URL must start with http:// or https://, not {self.API_URL!r}")
        if self.WORKER_KEY and self.WORKER_KEY_FILE:
            raise ValueError("Set WORKER_KEY or WORKER_KEY_FILE, not both")
        if not (self.WORKER_KEY or self.WORKER_KEY_FILE):
            raise ValueError("WORKER_KEY or WORKER_KEY_FILE is required")
        if not self.BASE_DIR and not (self.JOBS_DIR and self.ENGINES_DIR):
            raise ValueError("Set BASE_DIR, or both JOBS_DIR and ENGINES_DIR")
        if not (math.isfinite(self.POLLING_INTERVAL) and self.POLLING_INTERVAL >= 1):
            raise ValueError("POLLING_INTERVAL must be at least 1 second")
        if self.MAIL_SECURITY not in MAIL_SECURITY_MODES:
            raise ValueError(f"MAIL_SECURITY must be one of {', '.join(MAIL_SECURITY_MODES)}, "
                             f"not {self.MAIL_SECURITY!r}")
        # Half-configured mail would start without sending anything, unnoticed until an
        # email is missed.
        without_server = [name for name in ("MAIL_SENDER_MAIL", "MAIL_SENDER_NAME", "MAIL_USERNAME", "MAIL_PASSWORD",
                                            "MAIL_CA_FILE", "MAIL_RECEIVER_MAILS", "WORKER_HOSTNAME")
                          if getattr(self, name)]
        if not self.MAIL_SERVER and without_server:
            raise ValueError(f"MAIL_SERVER is required with {', '.join(without_server)}")
        if self.MAIL_SERVER and not self.MAIL_SENDER_MAIL:
            raise ValueError("MAIL_SENDER_MAIL is required with MAIL_SERVER")
        if self.MAIL_SERVER and not self.MAIL_RECEIVER_MAILS:
            raise ValueError("MAIL_RECEIVER_MAILS is required with MAIL_SERVER")
        # The login and the CA file are used only in some setups; elsewhere they would be ignored.
        if self.MAIL_USERNAME and not self.MAIL_PASSWORD:
            raise ValueError("MAIL_PASSWORD is required with MAIL_USERNAME: "
                             "the server is logged in to only with a password")
        tls = self.MAIL_SECURITY in ("ssl", "starttls") or (self.MAIL_SECURITY == "auto" and self.MAIL_PASSWORD)
        if self.MAIL_CA_FILE and not tls:
            raise ValueError("MAIL_CA_FILE needs TLS: MAIL_SECURITY ssl or starttls, or auto with a password")

    # An unset or empty variable keeps the default in all of these.
    def _raw(self, key: str) -> str:
        """The override for key if there is one, else the environment variable."""
        return self._overrides[key] if key in self._overrides else os.getenv(key, "")

    def _str(self, key: str, default: Optional[str]) -> Optional[str]:
        return self._raw(key) or default

    def _list(self, key: str) -> list:
        return [item.strip() for item in self._raw(key).split(",") if item.strip()]

    def _bool(self, key: str, default: bool) -> bool:
        value = self._raw(key).strip().lower()
        if not value:
            return default
        if value not in TRUE_VALUES | FALSE_VALUES:
            raise ValueError(f"{key} must be true or false, not {value!r}")
        return value in TRUE_VALUES

    def _number(self, key: str, default: float) -> float:
        value = self._raw(key) or str(default)
        try:
            return float(value)
        except ValueError:
            raise ValueError(f"{key} must be a number, not {value!r}") from None

    def _nonnegative_seconds(self, key: str, default: float) -> float:
        value = self._number(key, default)
        if not math.isfinite(value) or value < 0:
            raise ValueError(f"{key} must be a finite, nonnegative number")
        return value

    def _positive_seconds(self, key: str, default: float) -> float:
        value = self._number(key, default)
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"{key} must be a finite, positive number")
        return value

    def _nonnegative_int(self, key: str, default: int) -> int:
        value = self._raw(key) or str(default)
        try:
            number = int(value)
        except ValueError:
            raise ValueError(f"{key} must be a whole number, not {value!r}") from None
        if number < 0:
            raise ValueError(f"{key} must be a nonnegative integer")
        return number
