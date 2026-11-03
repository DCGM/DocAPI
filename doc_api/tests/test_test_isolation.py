"""Verify teardown using a child pytest run with deliberately failing tests."""
import ast
import re
import asyncio
import os
from pathlib import Path
import subprocess
import sys

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_failed_tests_drop_databases(isolated_database, tmp_path):
    # Copy the real fixtures into an independent suite. The parent test can then
    # inspect PostgreSQL after the child's failed assertion and setup failure.
    fixtures = Path(__file__).with_name("conftest.py")
    (tmp_path / "conftest.py").write_text(fixtures.read_text())
    (tmp_path / "test_child.py").write_text('''
from pathlib import Path
import pytest
import pytest_asyncio
from sqlalchemy import text
from doc_api.config import config

@pytest.mark.asyncio
async def test_assertion_failure(client, user_headers, isolated_database):
    Path(__file__).with_name("assertion_database").write_text(isolated_database.name)
    response = await client.get("/v1/me")
    assert response.status_code == 401
    response = await client.post("/v1/jobs", headers=user_headers,
        json={"images": [{"name": "page.jpg", "order": 0}]})
    assert response.status_code == 201, response.text
    marker = Path(config.JOBS_DIR) / "previous-test-file"
    marker.write_bytes(b"uploaded data")
    Path(__file__).with_name("first_jobs_dir").write_text(config.JOBS_DIR)
    assert False, "intentional assertion failure"

@pytest_asyncio.fixture
async def broken_setup(isolated_database):
    Path(__file__).with_name("setup_database").write_text(isolated_database.name)
    raise RuntimeError("intentional setup failure")

@pytest.mark.asyncio
async def test_setup_failure(broken_setup):
    pass

@pytest.mark.asyncio
async def test_next_test_is_empty(client, db_sessions):
    assert config.JOBS_DIR != Path(__file__).with_name("first_jobs_dir").read_text()
    assert not list(Path(config.JOBS_DIR).iterdir())
    async with db_sessions() as db:
        assert (await db.execute(text("SELECT count(*) FROM jobs"))).scalar_one() == 0
        assert (await db.execute(text("SELECT count(*) FROM keys"))).scalar_one() == 4
''')
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "log_cli=false",
         "-o", "asyncio_mode=strict", "-o", "asyncio_default_fixture_loop_scope=function", str(tmp_path)],
        cwd=tmp_path, env=os.environ.copy(), capture_output=True, text=True, timeout=90,
    )
    assert result.returncode == 1, result.stdout + result.stderr
    assert "1 failed, 1 passed, 1 error" in result.stdout, result.stdout + result.stderr
    names = [(tmp_path / filename).read_text() for filename in ("assertion_database", "setup_database")]
    assert names[0] != names[1]
    async with isolated_database.engine.connect() as db:
        for name in names:
            assert not (await db.execute(text("SELECT 1 FROM pg_database WHERE datname = :name"),
                                         {"name": name})).first()


def test_readme_references_current_tests():
    directory = Path(__file__).parent
    expected = {(path.name, node.name) for path in directory.glob("test_*.py")
                for node in ast.parse(path.read_text()).body
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name.startswith("test_")}
    documented = set(re.findall(r"(test_\w+\.py)::(test_\w+)", (directory / "README.md").read_text()))
    assert documented == expected, {"missing": expected - documented, "stale": documented - expected}
