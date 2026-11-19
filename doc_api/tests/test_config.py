""".env_example and .env_docs_example describe the settings config.py reads,
.env_docker_example the ones Compose reads for compose.yml, and config.py holds
only values."""
import ast
import json
import os
import re
import sys
from pathlib import Path

from doc_api import config as config_module
from doc_api.config import Config

SERVER_DIR = Path(config_module.__file__).parent
# The server's settings, in config.py's order when appended to each other.
SERVER_EXAMPLES = (".env_example", ".env_docs_example")
SETTING_LINE = re.compile(r"^(# )?([A-Z][A-Z0-9_]*)=(.*)$")


def read_settings(monkeypatch) -> dict:
    """Every setting Config reads from the environment, in the order it reads
    them, with its default."""
    real_getenv, real_env_bool = os.getenv, Config._env_bool
    read = {}

    def getenv(key, default=None):
        read.setdefault(key, default)
        return real_getenv(key, default)

    def env_bool(self, key, default=False):
        read.setdefault(key, "true" if default else "false")
        return real_env_bool(self, key, default)

    with monkeypatch.context() as patch:
        patch.setattr(os, "getenv", getenv)
        patch.setattr(Config, "_env_bool", env_bool)
        Config()
        for name in read:
            patch.delenv(name, raising=False)
        read.clear()
        Config()
    return read


def documented_settings(example: str = ".env_example") -> list[tuple[str, str, bool]]:
    """(name, value, commented out) for each setting of an example file. A value
    in single quotes may span lines, each commented out like the first."""
    lines = iter((SERVER_DIR / example).read_text().splitlines())
    settings = []
    for line in lines:
        match = SETTING_LINE.match(line)
        if not match:
            continue
        value = match[3]
        if value.startswith("'"):
            while len(value) < 2 or not value.endswith("'"):
                part = next(lines)
                if match[1]:
                    assert part.startswith("#"), f"{match[2]}: every line of a commented block starts with #"
                    part = part[2:] if part.startswith("# ") else part[1:]
                value += "\n" + part
        settings.append((match[2], value, bool(match[1])))
    return settings


def server_settings() -> list[tuple[str, str, bool]]:
    return [entry for example in SERVER_EXAMPLES for entry in documented_settings(example)]


def test_env_example_lists_every_setting_in_order_with_its_default(monkeypatch):
    read = read_settings(monkeypatch)
    documented = server_settings()
    assert [name for name, _, _ in documented] == list(read)
    for name, value, _ in documented:
        default = read[name]
        if value.startswith("<") and value.endswith(">"):
            # Described instead of shown: a setting without a default.
            assert default is None, f"{name} must show its default {default!r}"
            continue
        assert default is not None, f"{name} has no default; describe its value in <...>"
        if value.startswith("'"):
            value = value[1:-1]  # Taken as it is, as Compose does.
        else:
            value = re.sub(r"\$\{([A-Z0-9_]+)\}", lambda match: str(read[match[1]]), value.strip().strip('"'))
        if str(default).startswith("{"):
            assert json.loads(value) == json.loads(default), name
        else:
            assert value == str(default), name


def test_settings_the_image_sets_are_commented_out_in_env_example():
    # An active line in the deployment's .env would override the image,
    # e.g. PRODUCTION=true or the data directory.
    image = set(re.findall(r"^ENV ([A-Z][A-Z0-9_]*)=", (SERVER_DIR / "Dockerfile").read_text(), re.MULTILINE))
    active = {name for name, _, commented in server_settings() if not commented}
    assert not image & active


def test_env_docker_example_holds_what_compose_reads_and_no_server_settings(monkeypatch):
    # Compose fills ${...} in compose.yml from .env, where this file is appended.
    interpolated = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", (SERVER_DIR / "compose.yml").read_text()))
    compose = [name for name, _, _ in documented_settings(".env_docker_example")]
    assert sorted(compose) == sorted(interpolated)
    assert not set(compose) & set(read_settings(monkeypatch))


def test_app_base_url_is_joined_as_a_url(monkeypatch):
    for name in ("APP_BASE_URL", "APP_HOST", "APP_PORT", "APP_URL_ROOT"):
        monkeypatch.delenv(name, raising=False)
    assert Config().APP_BASE_URL == "http://0.0.0.0:9999"
    monkeypatch.setenv("APP_URL_ROOT", "/subpath")
    assert Config().APP_BASE_URL == "http://0.0.0.0:9999/subpath"
    monkeypatch.setenv("APP_BASE_URL", "https://docapi.example.org/")
    assert Config().APP_BASE_URL == "https://docapi.example.org"


def test_config_imports_only_the_standard_library():
    tree = ast.parse(Path(config_module.__file__).read_text())
    modules = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import)
               for alias in node.names}
    modules |= {node.module.split(".")[0] for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.level == 0}
    assert modules <= sys.stdlib_module_names, modules - sys.stdlib_module_names
