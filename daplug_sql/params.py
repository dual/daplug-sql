from __future__ import annotations

import json
from typing import Any, Sequence, Tuple

from psycopg2.extras import Json  # type: ignore[import-untyped]


def adapt_value(engine: str, value: Any) -> Any:
    if not isinstance(value, (dict, list)):
        return value
    if engine == 'mysql':
        return json.dumps(value)
    return Json(value)


def adapt_sequence(engine: str, values: Sequence[Any]) -> Tuple[Any, ...]:
    return tuple(adapt_value(engine, value) for value in values)
