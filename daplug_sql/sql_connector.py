from __future__ import annotations

from typing import Any

import psycopg2  # type: ignore[import-untyped]
from psycopg2.extras import RealDictCursor  # type: ignore[import-untyped]

import mysql.connector

from .types import AdapterConfig, ConnectionProtocol, CursorProtocol


class SQLConnector:

    def __init__(self, cls: AdapterConfig) -> None:
        self.endpoint: str = cls.endpoint
        self.database: str = cls.database
        self.table: str | None = getattr(cls, 'table', None)
        self.user: str = cls.user
        self.password: str = cls.password
        self.port: int = cls.port
        self.autocommit: bool = getattr(cls, 'autocommit', False)
        self.engine: str = getattr(cls, 'engine', 'postgres').lower()
        self.connection: Any = None

    def connect(self) -> ConnectionProtocol:
        if self.engine == 'mysql':
            return self._connect_mysql()
        return self._connect_postgres()

    def cursor(self) -> CursorProtocol:
        connection = self.connect()
        if self.engine == 'mysql':
            return connection.cursor(dictionary=True)
        return connection.cursor(cursor_factory=RealDictCursor)

    def _connect_postgres(self) -> ConnectionProtocol:
        if not self.connection or self.connection.closed:
            self.connection = psycopg2.connect(
                dbname=self.database,
                host=self.endpoint,
                port=self.port,
                user=self.user,
                password=self.password,
            )
            if self.autocommit:
                self.connection.set_session(autocommit=self.autocommit)
        return self.connection

    def _connect_mysql(self) -> ConnectionProtocol:
        is_connected = getattr(self.connection, 'is_connected', lambda: False)
        if not self.connection or not is_connected():
            self.connection = mysql.connector.connect(
                host=self.endpoint,
                user=self.user,
                password=self.password,
                database=self.database,
                port=self.port,
                charset='utf8mb4',
            )
        self.connection.autocommit = self.autocommit
        return self.connection
