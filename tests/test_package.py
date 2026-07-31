from pathlib import Path

import tomllib


ROOT = Path(__file__).parents[1]


def test_distribution_metadata_and_console_entry_point() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text())
    project = metadata["project"]
    assert project["requires-python"] == ">=3.14"
    assert project["dependencies"] == ["mcp>=2.0.0,<3"]
    assert project["scripts"]["serena-shared"] == "serena_shared.cli:main"
