from __future__ import annotations

from collections.abc import Mapping
from typing import TypeGuard


def is_json_object(value: object) -> TypeGuard[Mapping[str, object]]:
    """Narrow a decoded JSON object to the read-only mapping interface."""
    return isinstance(value, dict)
