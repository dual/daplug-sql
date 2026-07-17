from __future__ import annotations

import json
from typing import Any, Sequence, Tuple

from psycopg2.extras import Json  # type: ignore[import-untyped]


class ParamAdapter:

    def __init__(self, engine: str) -> None:
        self.engine: str = engine.lower()

    def value(self, value: Any) -> Any:
        if not isinstance(value, (dict, list)):
            return value
        if self.engine == 'mysql':
            return json.dumps(value)
        return Json(value)

    def sequence(self, values: Sequence[Any]) -> Tuple[Any, ...]:
        return tuple(self.value(value) for value in values)
