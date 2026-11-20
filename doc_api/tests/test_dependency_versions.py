"""The server's versions are pinned in doc_api/requirements.txt, which the Docker
image installs; the package (pyproject.toml) leaves the versions to the projects
that use DocAPI and requires only the Python the image runs."""
import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).parents[2]


def test_pyproject_pins_no_versions():
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    requirements = project["dependencies"] + [requirement
                                              for extra in project["optional-dependencies"].values()
                                              for requirement in extra]
    pinned = [requirement for requirement in requirements if re.search(r"[<>=!~;@]", requirement)]
    assert not pinned, "pyproject.toml names packages only; the server's versions go in doc_api/requirements.txt"


def test_docker_image_runs_the_required_python():
    required = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["requires-python"]
    image = re.search(r"^FROM python:(\d+\.\d+)-", (ROOT / "doc_api" / "Dockerfile").read_text(), re.MULTILINE)
    assert image and required == f">={image[1]}"
