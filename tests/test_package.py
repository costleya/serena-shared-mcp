from importlib.resources import files
from pathlib import Path

import tomllib


ROOT = Path(__file__).parents[1]


def test_distribution_metadata_and_console_entry_point() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text())
    project = metadata["project"]
    assert project["requires-python"] == ">=3.14"
    assert project["dependencies"] == ["mcp>=2.0.0,<3"]
    assert project["scripts"]["serena-shared"] == "serena_shared.cli:main"


def test_packaged_serena_context_is_available() -> None:
    context = files("serena_shared.resources").joinpath("serena.yml")
    assert context.is_file()
    assert "activate_project" in context.read_text()
