from typing import Any

from .adapter import SQLAdapter


def adapter(**kwargs: Any) -> SQLAdapter:
    return SQLAdapter(**kwargs)


__all__ = ['SQLAdapter', 'adapter']
