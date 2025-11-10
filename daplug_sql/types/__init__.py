"""Shared typing helpers for daplug_sql."""
from __future__ import annotations

from typing import Any, Dict, Iterable, Protocol, Sequence, Tuple, Union, runtime_checkable

JSONDict = Dict[str, Any]
Params = Union[Sequence[Any], Tuple[Any, ...]]
StrIterable = Iterable[str]


@runtime_checkable
class CursorProtocol(Protocol):
    def execute(self, query: str, params: Sequence[Any] | None = ...) -> Any: ...

    def fetchone(self) -> JSONDict | None: ...

    def fetchall(self) -> Sequence[JSONDict]: ...

    def close(self) -> None: ...

    def mogrify(self, query: str, params: Sequence[Any] | None = ...) -> bytes | str: ...


class ConnectionProtocol(Protocol):
    autocommit: bool

    def cursor(self, *args: Any, **kwargs: Any) -> CursorProtocol: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...

    @property
    def closed(self) -> Any: ...

    def set_session(self, *args: Any, **kwargs: Any) -> None: ...

    def is_connected(self) -> bool: ...


class AdapterConfig(Protocol):
    endpoint: str
    database: str
    table: str | None
    user: str
    password: str
    port: int
    autocommit: bool
    engine: str


__all__ = [
    'AdapterConfig',
    'ConnectionProtocol',
    'CursorProtocol',
    'JSONDict',
    'Params',
    'StrIterable',
]
