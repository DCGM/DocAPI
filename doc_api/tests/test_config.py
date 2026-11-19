""".env_example describes the settings config.py reads, .env_docker_example the
ones Compose reads for compose.yml, and config.py holds only values."""
import ast
import json
import os
import re
import sys
from pathlib import Path

from doc_api import config as config_module
from doc_api.config import Config

SERVER_DIR = Path(config_module.__file__).parent
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
    """(name, value, commented out) for each setting line of an example file."""
    lines = (SERVER_DIR / example).read_text().splitlines()
    return [(match[2], match[3], bool(match[1])) for match in map(SETTING_LINE.match, lines) if match]


def test_env_example_lists_every_setting_in_order_with_its_default(monkeypatch):
    read = read_settings(monkeypatch)
    documented = documented_settings()
    assert [name for name, _, _ in documented] == list(read)
    for name, value, _ in documented:
        default = read[name]
        if value.startswith("<") and value.endswith(">"):
            # Described instead of shown: no default, or a multi-line text.
            assert default is None or "\n" in str(default), f"{name} must show its default {default!r}"
            continue
        assert default is not None, f"{name} has no default; describe its value in <...>"
        value = re.sub(r"\$\{([A-Z0-9_]+)\}", lambda match: str(read[match[1]]), value.strip().strip('"'))
        if str(default).startswith("{"):
            assert json.loads(value) == json.loads(default), name
        else:
            assert value == str(default), name


def test_settings_the_image_sets_are_commented_out_in_env_example():
    # An active line in the deployment's .env would override the image,
    # e.g. PRODUCTION=true or the data directory.
    image = set(re.findall(r"^ENV ([A-Z][A-Z0-9_]*)=", (SERVER_DIR / "Dockerfile").read_text(), re.MULTILINE))
    active = {name for name, _, commented in documented_settings() if not commented}
    assert not image & active


def test_env_docker_example_holds_what_compose_reads_and_no_server_settings(monkeypatch):
    # Compose fills ${...} in compose.yml from .env, where this file is appended.
    interpolated = set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)", (SERVER_DIR / "compose.yml").read_text()))
    compose = [name for name, _, _ in documented_settings(".env_docker_example")]
    assert sorted(compose) == sorted(interpolated)
    assert not set(compose) & set(read_settings(monkeypatch))


def test_config_imports_only_the_standard_library():
    tree = ast.parse(Path(config_module.__file__).read_text())
    modules = {alias.name.split(".")[0] for node in ast.walk(tree) if isinstance(node, ast.Import)
               for alias in node.names}
    modules |= {node.module.split(".")[0] for node in ast.walk(tree)
                if isinstance(node, ast.ImportFrom) and node.level == 0}
    assert modules <= sys.stdlib_module_names, modules - sys.stdlib_module_names
