"""The server's pinned versions (doc_api/requirements.txt, used by the Docker
image) and the package's (pyproject.toml) name the same versions of the
packages they share, and the image runs the Python the package requires."""
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[2]
SHARED = ("fastapi", "starlette", "pydantic", "numpy", "opencv-python-headless")


def _versions(requirements, operator):
    """{name: version} of the shared packages pinned with operator."""
    found = {}
    for requirement in requirements:
        match = re.fullmatch(r"\s*([A-Za-z0-9_.-]+)(\[[^\]]*\])?\s*" + re.escape(operator) + r"\s*([^\s;,]+)\s*",
                             requirement)
        if match and match[1].lower() in SHARED:
            found[match[1].lower()] = match[3]
    return found


def test_requirements_and_pyproject_pin_the_same_shared_packages():
    server = _versions((ROOT / "doc_api" / "requirements.txt").read_text().splitlines(), "==")
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    package = _versions(project["dependencies"] + project["optional-dependencies"]["server"], "~=")
    assert set(server) == set(SHARED), "requirements.txt pins each exactly"
    assert package == server


def test_docker_image_runs_the_required_python():
    required = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["requires-python"]
    image = re.search(r"^FROM python:(\d+\.\d+)-", (ROOT / "doc_api" / "Dockerfile").read_text(), re.MULTILINE)
    assert image and required == f">={image[1]}"
