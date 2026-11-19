"""The worker config and DocWorkerWrapper.from_config: defaults, the environment,
overrides, .env_example and the dummy worker's arguments."""
import inspect
import os
import re
import ssl
from pathlib import Path
from unittest import mock

import pytest

from doc_api.connector import DEFAULT_TIMEOUT
from doc_api.tools.mail_sender import SECURITY_MODES, MailSender
from doc_worker import config as config_module
from doc_worker import dummy_worker
from doc_worker.config import MAIL_SECURITY_MODES, Config
from doc_worker.doc_worker_wrapper import DEFAULT_WORKER_ERRORS, DocWorkerWrapper

CONFIG_SOURCE = Path(config_module.__file__).read_text()
ENV_EXAMPLE = Path(__file__).parents[1] / ".env_example"
# The variables Config reads, in the order it reads them.
READ_NAMES = re.findall(r'self\._\w+\("([A-Z_]+)"', inspect.getsource(Config.read))
ERRORS = {name: text for key, message in DEFAULT_WORKER_ERRORS.items()
          for name, text in ((f"ERROR_{key.upper()}_LOG", message.final_log),
                             (f"ERROR_{key.upper()}_LOG_USER", message.final_log_user))}

WRAPPER = inspect.signature(DocWorkerWrapper).parameters
MAIL = inspect.signature(MailSender).parameters
# Each setting with the default of the argument it feeds.
CODE_DEFAULTS = {
    "API_CONNECT_TIMEOUT_SECONDS": DEFAULT_TIMEOUT[0],
    "API_READ_TIMEOUT_SECONDS": DEFAULT_TIMEOUT[1],
    **{name.upper(): WRAPPER[name].default for name in (
        "base_dir", "job_history_path", "jobs_dir", "cleanup_job_dir", "engines_dir", "cleanup_old_engines",
        "polling_interval", "prefer_image_url", "prefer_alto_url", "prefer_page_url", "mail_subject",
        "alive_mail_startup", "alive_mail_interval_seconds", "outage_grace_seconds",
        "outage_mail_interval_seconds")},
    "MAIL_RECEIVER_MAILS": list(WRAPPER["mail_receivers"].default),
    "WORKER_IDENTITY": WRAPPER["identity"].default,
    "WORKER_HOSTNAME": WRAPPER["hostname"].default,
    **{f"MAIL_{name.upper()}": MAIL[name].default for name in (
        "port", "sender_name", "username", "password", "security", "ca_file", "send_receiver_limit",
        "send_receiver_window_seconds", "send_server_limit", "send_server_window_seconds")},
    "MAIL_TIMEOUT_SECONDS": MAIL["timeout"].default,
    **ERRORS,
}
# Settings with no default in the code they feed: required, or off when unset.
NOT_SET = {"API_URL": None, "WORKER_KEY": None, "WORKER_KEY_FILE": None, "MAIL_SERVER": None,
           "MAIL_SENDER_MAIL": None}
REQUIRED = ("API_URL", "WORKER_KEY", "BASE_DIR")
MAIL_SETUP = {"MAIL_SERVER": "smtp.example.org", "MAIL_SENDER_MAIL": "worker@example.org",
              "MAIL_RECEIVER_MAILS": "admin@example.org"}


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    """No setting from the developer's shell reaches these tests; the required ones are set."""
    for name in READ_NAMES:
        monkeypatch.delenv(name, raising=False)
    set_env(monkeypatch, {"API_URL": "https://docapi.example.org", "WORKER_KEY": "k", "BASE_DIR": str(tmp_path)})
    return monkeypatch


def settings(config):
    return {name: value for name, value in vars(config).items() if name.isupper()}


def set_env(monkeypatch, values):
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_config_only_reads_the_environment():
    """The config imports nothing of DocAPI or the wrapper; the wrapper builds on it, not the reverse."""
    imports = re.findall(r"^(?:from (\S+) import|import (\S+))", CONFIG_SOURCE, re.MULTILINE)
    imports = [module for found in imports for module in found if module]
    assert set(imports) <= {"math", "os", "typing"}
    assert MAIL_SECURITY_MODES == SECURITY_MODES


def test_config_covers_every_argument():
    """Every argument of the wrapper and of MailSender has a setting; the objects built from them do not."""
    renamed = {"mail_receivers": "MAIL_RECEIVER_MAILS", "identity": "WORKER_IDENTITY",
               "hostname": "WORKER_HOSTNAME"}
    wrapper = {renamed.get(name, name.upper()) for name in WRAPPER} - {
        "SELF", "CONNECTOR", "MAIL_SENDER", "ERROR_MESSAGES"}
    # Like the server's mail settings: no extra headers, and the charset and content type
    # stay MailSender's, as the worker's emails are plain text.
    mail = {"MAIL_TIMEOUT_SECONDS" if name == "timeout" else f"MAIL_{name.upper()}" for name in MAIL} - {
        "MAIL_HEADERS", "MAIL_CHARSET", "MAIL_CONTENT_TYPE"}
    names = set(settings(Config()))
    # error_messages: two settings per message.
    assert wrapper <= names and mail <= names and set(ERRORS) <= names
    assert names == set(CODE_DEFAULTS) | set(NOT_SET) == set(READ_NAMES)


def test_defaults_match_the_code():
    config = settings(Config())
    expected = {**CODE_DEFAULTS, **NOT_SET}
    assert {name: value for name, value in config.items() if name not in REQUIRED} == {
        name: value for name, value in expected.items() if name not in REQUIRED}


def test_env_example_matches_config(clean_env):
    example = re.findall(r"^([A-Z_]+)=(.*)$", ENV_EXAMPLE.read_text(), re.MULTILINE)
    assert [name for name, _ in example] == READ_NAMES
    # Every documented value is the default: reading the file changes nothing but the
    # required settings, which have none.
    required = {name: os.environ[name] for name in REQUIRED}
    defaults = settings(Config())
    set_env(clean_env, dict(example))
    assert settings(Config(required)) == defaults


def test_environment_values_are_parsed(clean_env):
    set_env(clean_env, {"API_CONNECT_TIMEOUT_SECONDS": "2.5", "CLEANUP_JOB_DIR": "Yes", "PREFER_PAGE_URL": "0",
                        "MAIL_PORT": "587", **MAIL_SETUP, "MAIL_RECEIVER_MAILS": " a@example.org, ,b@example.org ",
                        "POLLING_INTERVAL": "", "MAIL_SUBJECT": "{status}"})
    config = Config()
    assert config.API_URL == "https://docapi.example.org"
    assert config.API_CONNECT_TIMEOUT_SECONDS == 2.5
    assert config.CLEANUP_JOB_DIR is True and config.PREFER_PAGE_URL is False
    assert config.MAIL_PORT == 587
    assert config.MAIL_RECEIVER_MAILS == ["a@example.org", "b@example.org"]
    assert config.POLLING_INTERVAL == 5  # An empty value keeps the default.
    assert config.MAIL_SUBJECT == "{status}"


def test_overrides_replace_the_environment(clean_env):
    set_env(clean_env, {"POLLING_INTERVAL": "7", "CLEANUP_JOB_DIR": "true", "WORKER_IDENTITY": "Env worker"})
    config = Config({"POLLING_INTERVAL": "2", "CLEANUP_JOB_DIR": "false", "WORKER_IDENTITY": ""})
    assert config.POLLING_INTERVAL == 2.0 and config.CLEANUP_JOB_DIR is False
    assert config.WORKER_IDENTITY is None  # An empty override is unset: the default, here none.
    assert Config({"POLLING_INTERVAL": ""}).POLLING_INTERVAL == 5  # Unset: the default.
    assert config.API_URL == "https://docapi.example.org"  # Not overridden: read from the environment.
    with pytest.raises(ValueError, match="POLLING_INTERVAL must be at least 1 second"):
        Config({"POLLING_INTERVAL": "0.5"})  # Overrides are parsed and checked like the environment.
    with pytest.raises(TypeError, match="Unknown settings: POLLING_INTERVALL"):
        Config({"POLLING_INTERVALL": "2"})
    for value in (2, None):
        with pytest.raises(TypeError, match="must be text"):
            Config({"POLLING_INTERVAL": value})


@pytest.mark.parametrize("environ, message", [
    ({"API_URL": ""}, "API_URL is required"),
    ({"API_URL": "docapi.example.org"}, "API_URL must start with http:// or https://"),
    ({"WORKER_KEY": ""}, "WORKER_KEY or WORKER_KEY_FILE is required"),
    ({"WORKER_KEY_FILE": "f"}, "Set WORKER_KEY or WORKER_KEY_FILE, not both"),
    ({"BASE_DIR": ""}, "Set BASE_DIR, or both JOBS_DIR and ENGINES_DIR"),
    ({"BASE_DIR": "", "JOBS_DIR": "jobs"}, "Set BASE_DIR, or both JOBS_DIR and ENGINES_DIR"),
    ({"MAIL_PORT": "25.5"}, "MAIL_PORT must be a whole number"),
    ({"POLLING_INTERVAL": "soon"}, "POLLING_INTERVAL must be a number"),
    ({"CLEANUP_JOB_DIR": "maybe"}, "CLEANUP_JOB_DIR must be true or false"),
    ({"API_READ_TIMEOUT_SECONDS": "0"}, "API_READ_TIMEOUT_SECONDS must be a finite, positive number"),
    *(({"POLLING_INTERVAL": value}, "POLLING_INTERVAL must be at least 1 second")
      for value in ("0", "0.5", "-1", "inf")),
    ({"OUTAGE_MAIL_INTERVAL_SECONDS": "inf"}, "OUTAGE_MAIL_INTERVAL_SECONDS must be a finite"),
    ({"MAIL_SEND_SERVER_LIMIT": "-1"}, "MAIL_SEND_SERVER_LIMIT must be a nonnegative integer"),
    ({"MAIL_SECURITY": "tls"}, "MAIL_SECURITY must be one of"),
    ({"MAIL_SERVER": "smtp.example.org", "MAIL_RECEIVER_MAILS": "a@example.org"},
     "MAIL_SENDER_MAIL is required with MAIL_SERVER"),
    ({"MAIL_SERVER": "smtp.example.org", "MAIL_SENDER_MAIL": "worker@example.org"},
     "MAIL_RECEIVER_MAILS is required with MAIL_SERVER"),
    ({"MAIL_RECEIVER_MAILS": "a@example.org"}, "MAIL_SERVER is required with MAIL_RECEIVER_MAILS"),
    ({"MAIL_SENDER_MAIL": "worker@example.org", "MAIL_PASSWORD": "p"},
     "MAIL_SERVER is required with MAIL_SENDER_MAIL, MAIL_PASSWORD"),
    *(({name: "x"}, f"MAIL_SERVER is required with {name}")
      for name in ("MAIL_SENDER_NAME", "MAIL_USERNAME", "MAIL_CA_FILE", "WORKER_HOSTNAME")),
    ({**MAIL_SETUP, "MAIL_USERNAME": "worker"}, "MAIL_PASSWORD is required with MAIL_USERNAME"),
    ({**MAIL_SETUP, "MAIL_CA_FILE": "ca.pem"}, "MAIL_CA_FILE needs TLS"),
    ({**MAIL_SETUP, "MAIL_CA_FILE": "ca.pem", "MAIL_SECURITY": "none", "MAIL_PASSWORD": "p"}, "MAIL_CA_FILE needs TLS"),
])
def test_invalid_settings_fail_when_the_config_is_built(clean_env, environ, message):
    set_env(clean_env, environ)
    with pytest.raises(ValueError, match=re.escape(message)):
        Config()


def test_from_config_builds_a_worker_without_mail(clean_env, tmp_path):
    key_file = tmp_path / "key"
    key_file.write_text("secret-key\n")
    worker = dummy_worker.DummyWorker.from_config(Config({
        "WORKER_IDENTITY": "Test worker", "WORKER_KEY": "", "WORKER_KEY_FILE": str(key_file),
        "BASE_DIR": str(tmp_path / "data"), "API_READ_TIMEOUT_SECONDS": "30", "POLLING_INTERVAL": "2"}))
    assert worker._connector.api_key == "secret-key"
    assert worker._connector.timeout == (10, 30)
    assert worker.mail is None and not worker.alive.on
    assert worker.identity == "Test worker" and worker.polling_interval == 2
    assert worker.jobs_dir == str(tmp_path / "data" / "jobs")
    assert worker.error_messages == dict(DEFAULT_WORKER_ERRORS)


def test_from_config_reads_each_error_message(clean_env):
    worker = dummy_worker.DummyWorker.from_config(
        Config({"ERROR_DOWNLOAD_INPUT_LOG_USER": "Could not download the {kind} {filename}"}))
    message = worker.error_messages["download_input"]
    assert message.final_log == DEFAULT_WORKER_ERRORS["download_input"].final_log
    assert message.final_log_user == "Could not download the {kind} {filename}"


def test_from_config_builds_mail_and_alive_reports(clean_env):
    worker = dummy_worker.DummyWorker.from_config(Config({
        **MAIL_SETUP, "MAIL_PASSWORD": "mail-password", "MAIL_SEND_SERVER_LIMIT": "5",
        "ALIVE_MAIL_INTERVAL_SECONDS": "0"}))
    # Building a sender opens no connection; the root conftest fails any that is opened.
    sender = worker.mail.sender
    assert (sender.server, sender.sender_mail, sender.password) == ("smtp.example.org", "worker@example.org",
                                                                   "mail-password")
    assert sender.security == "ssl"  # auto with a password
    assert sender.send_server_limit == 5
    assert worker.mail.receivers == ["admin@example.org"]
    assert worker.alive.on and (worker.alive.mail_startup, worker.alive.mail_interval_seconds) == (True, 0)


@pytest.mark.parametrize("extra", [
    {"MAIL_CA_FILE": "CA", "MAIL_SECURITY": "starttls"},
    {"MAIL_CA_FILE": "CA", "MAIL_PASSWORD": "p"},  # auto with a password is ssl
    {"MAIL_USERNAME": "worker", "MAIL_PASSWORD": "p", "MAIL_SECURITY": "none"},  # allowed, as by MailSender
])
def test_complete_mail_setups_are_accepted(clean_env, extra):
    if extra.get("MAIL_CA_FILE"):
        extra = {**extra, "MAIL_CA_FILE": ssl.get_default_verify_paths().cafile or ""}
        if not extra["MAIL_CA_FILE"]:
            pytest.skip("no system CA file to load")
    assert dummy_worker.DummyWorker.from_config(Config({**MAIL_SETUP, **extra})).mail is not None


class OwnConfig(Config):
    def read(self):
        super().read()
        self.MODEL_PATH = self._str("MODEL_PATH", "model.bin")

    def check(self):
        super().check()
        if not self.MODEL_PATH.endswith(".bin"):
            raise ValueError("MODEL_PATH must be a .bin file")


class OwnWorker(dummy_worker.DummyWorker):
    def __init__(self, *args, model_path, mail_password="own", **kwargs):
        super().__init__(*args, **kwargs)
        self.model_path = model_path
        self.own_mail_password = mail_password


class ChildWorker(OwnWorker):
    """Takes model_path through its parent's constructor."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class LoadingWorker(dummy_worker.DummyWorker):
    """Builds its model from MODEL_PATH before the worker exists."""
    def __init__(self, *args, model, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = model

    @classmethod
    def subclass_args(cls, config):
        return {"model": f"loaded {config.MODEL_PATH}"}


class WrapperSettingWorker(dummy_worker.DummyWorker):
    @classmethod
    def subclass_args(cls, config):
        return {"polling_interval": 1}


def test_a_subclassed_config_reads_and_checks_its_own_settings(clean_env):
    assert OwnConfig().MODEL_PATH == "model.bin"
    clean_env.setenv("MODEL_PATH", "env.bin")
    assert OwnConfig().MODEL_PATH == "env.bin"
    config = OwnConfig({"MODEL_PATH": "other.bin", "POLLING_INTERVAL": "2"})
    assert (config.MODEL_PATH, config.POLLING_INTERVAL) == ("other.bin", 2)
    assert OwnConfig({"MODEL_PATH": ""}).MODEL_PATH == "model.bin"
    assert not hasattr(config, "_overrides")
    with pytest.raises(TypeError, match="Unknown settings: MODEL_PAHT"):
        OwnConfig({"MODEL_PAHT": "other.bin"})
    with pytest.raises(ValueError, match="MODEL_PATH must be a .bin file"):
        OwnConfig({"MODEL_PATH": "model.txt"})
    with pytest.raises(ValueError, match="POLLING_INTERVAL"):  # The wrapper's checks still run.
        OwnConfig({"POLLING_INTERVAL": "0"})
    with pytest.raises(TypeError, match="Unknown settings: MODEL_PATH"):  # Only the subclass has it.
        Config({"MODEL_PATH": "other.bin"})


def test_from_config_passes_a_subclass_its_own_settings(clean_env):
    config = OwnConfig({"MODEL_PATH": "other.bin", "POLLING_INTERVAL": "2"})
    worker = OwnWorker.from_config(config)
    assert (worker.model_path, worker.polling_interval) == ("other.bin", 2)
    assert ChildWorker.from_config(config).model_path == "other.bin"


def test_from_config_keeps_the_settings_of_config_for_the_wrapper(clean_env):
    worker = OwnWorker.from_config(OwnConfig({**MAIL_SETUP, "MAIL_PASSWORD": "secret"}))
    assert (worker.model_path, worker.own_mail_password) == ("model.bin", "own")
    assert worker.mail is not None


def test_a_setting_without_a_parameter_fails(clean_env):
    with pytest.raises(TypeError, match="OwnConfig settings without a parameter of DummyWorker: MODEL_PATH"):
        dummy_worker.DummyWorker.from_config(OwnConfig())
    with pytest.raises(TypeError, match="model_path"):  # A plain Config has no MODEL_PATH.
        OwnWorker.from_config(Config())


def test_an_overridden_subclass_args_builds_its_own_arguments(clean_env):
    worker = LoadingWorker.from_config(OwnConfig({**MAIL_SETUP, "MODEL_PATH": "other.bin"}))
    assert worker.model == "loaded other.bin"
    assert worker.mail is not None and worker.alive.on  # Built as usual.
    with pytest.raises(TypeError, match="multiple values"):
        WrapperSettingWorker.from_config(Config())


def test_setting_names_are_what_read_sets(clean_env):
    clean_env.setenv("POLLING_INTERVAL", "0")  # Not read, and so not checked.
    assert list(Config.setting_names()) == READ_NAMES
    assert OwnConfig.setting_names() == (*READ_NAMES, "MODEL_PATH")


def test_dummy_worker_arguments_override_the_environment(clean_env, tmp_path):
    set_env(clean_env, {"WORKER_IDENTITY": "Env worker", "WORKER_KEY": "", "WORKER_KEY_FILE": str(tmp_path / "key"),
                        "POLLING_INTERVAL": "7", "CLEANUP_JOB_DIR": "true", "API_READ_TIMEOUT_SECONDS": "30"})
    assert settings(dummy_worker.build_config(dummy_worker.parse_args([]))) == settings(Config())
    config = dummy_worker.build_config(dummy_worker.parse_args([
        "--worker-identity", "Args worker", "--api-url", "https://args.example.org", "--api-key", "argument-key",
        "--polling-interval", "1", "--no-cleanup-job-dir", "--connect-timeout", "3"]))
    assert (config.WORKER_IDENTITY, config.API_URL) == ("Args worker", "https://args.example.org")
    assert (config.WORKER_KEY, config.WORKER_KEY_FILE) == ("argument-key", None)
    assert config.POLLING_INTERVAL == 1.0
    assert config.CLEANUP_JOB_DIR is False
    assert config.API_CONNECT_TIMEOUT_SECONDS == 3.0
    assert config.API_READ_TIMEOUT_SECONDS == 30.0  # Not given, so the environment's value stays.
    assert config.BASE_DIR == str(tmp_path)
    with pytest.raises(ValueError, match="POLLING_INTERVAL must be at least 1 second"):
        dummy_worker.build_config(dummy_worker.parse_args(["--polling-interval", "0.5"]))


@pytest.mark.parametrize("environ,state", [({}, "off"), (MAIL_SETUP, "on")])
def test_the_dummy_worker_starts_from_its_command_line(clean_env, monkeypatch, caplog, environ, state):
    set_env(clean_env, environ)
    monkeypatch.setattr("sys.argv", ["dummy_worker", "--polling-interval", "2"])
    with mock.patch.object(dummy_worker.DummyWorker, "start") as start, caplog.at_level("INFO"):
        dummy_worker.main()
    start.assert_called_once()
    assert f"Worker mail: {state}" in caplog.text
