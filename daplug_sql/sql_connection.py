from __future__ import annotations

import threading
from typing import Any, Callable, Dict, Tuple

from .sql_connector import SQLConnector
from .types import AdapterConfig, ConnectionProtocol

CacheKey = Tuple[str, str, str, int, str]
_connection_cache: Dict[CacheKey, SQLConnector] = {}
_cache_lock = threading.Lock()


def _build_cache_key(obj: AdapterConfig) -> CacheKey:
    return (obj.endpoint, obj.database, obj.user, obj.port, getattr(obj, 'engine', 'postgres'))


def _is_connection_closed(connection: ConnectionProtocol | None) -> bool:
    if not connection:
        return True
    closed_attr = getattr(connection, 'closed', None)
    if closed_attr is not None:
        return bool(closed_attr)
    open_attr = getattr(connection, 'open', None)
    if open_attr is not None:
        return open_attr == 0
    is_connected = getattr(connection, 'is_connected', None)
    if callable(is_connected):
        return not is_connected()
    return False


def sql_connection(func: Callable[..., Any]) -> Callable[..., Any]:

    def decorator(obj: AdapterConfig, *args: Any, **kwargs: Any) -> Any:
        cache_key = _build_cache_key(obj)
        with _cache_lock:
            connector = _connection_cache.get(cache_key)
            if not connector or _is_connection_closed(connector.connection):
                connector = SQLConnector(obj)
                _connection_cache[cache_key] = connector
        return func(obj, connector, *args, **kwargs)

    return decorator


def sql_connection_cleanup(func: Callable[..., Any]) -> Callable[..., Any]:

    def decorator(obj: AdapterConfig, *args: Any, **kwargs: Any) -> Any:
        try:
            return func(obj, *args, **kwargs)
        finally:
            _close_connectors_for(obj)

    return decorator


def _close_connectors_for(obj: AdapterConfig | None = None) -> None:
    connectors: list[SQLConnector] = []
    with _cache_lock:
        if obj is None:
            connectors = list(_connection_cache.values())
            _connection_cache.clear()
        else:
            cache_key = _build_cache_key(obj)
            connector = _connection_cache.pop(cache_key, None)
            if connector:
                connectors.append(connector)
    for connector in connectors:
        connection = getattr(connector, 'connection', None)
        if connection:
            try:
                connection.close()
            except Exception:
                pass
        connector.connection = None
