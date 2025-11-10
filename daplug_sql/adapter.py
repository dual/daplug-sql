from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any, Optional, Sequence, Tuple

from daplug_core import dict_merger  # type: ignore[import-untyped]
from daplug_core.base_adapter import BaseAdapter  # type: ignore[import-untyped]
from daplug_core.logger import logger  # type: ignore[import-untyped]

from .exception import CreateTableException, SQLAdapterException
from .sql_connection import sql_connection, sql_connection_cleanup
from .types import ConnectionProtocol, CursorProtocol, JSONDict

if TYPE_CHECKING:
    from .sql_connector import SQLConnector


class SQLAdapter(BaseAdapter):

    SAFE_IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.endpoint: str = kwargs['endpoint']
        self.database: str = kwargs['database']
        self.user: str = kwargs['user']
        self.password: str = kwargs['password']
        self.port: int = kwargs.get('port', 5432)
        self.engine: str = kwargs.get('engine', 'postgres').lower()
        self.autocommit: bool = kwargs.get('autocommit', True)
        self.connection: ConnectionProtocol | None = None
        self.cursor: CursorProtocol | None = None

    @sql_connection
    def connect(self, connector: 'SQLConnector') -> None:
        self.connection = connector.connect()
        self.cursor = connector.cursor()

    @sql_connection_cleanup
    def close(self) -> None:
        self.__close_cursor()
        self.__close_connection()

    def commit(self, commit: bool = True) -> None:
        if commit and self.connection:
            self.connection.commit()

    def create(self, **kwargs: Any) -> JSONDict:
        return self.insert(**kwargs)

    def insert(self, **kwargs: Any) -> JSONDict:
        data, columns, values = self.__get_data_params(**kwargs)
        table = self.__format_identifier(kwargs['table'])
        formatted_columns = [self.__format_identifier(column) for column in columns]
        placeholder_clause = self.__build_placeholders(len(columns))
        query = f'INSERT INTO {table} ({", ".join(formatted_columns)}) VALUES ({placeholder_clause})'
        exists = self.__get_existing(**kwargs)
        if exists:
            self.__raise_error('NOT_UNIQUE', **kwargs)
        self.__execute(query, values, **kwargs)
        super().publish(data, **kwargs)
        return data

    def read(self, identifier_value: Any, **kwargs: Any) -> Optional[JSONDict]:
        return self.get(identifier_value, **kwargs)

    def get(self, identifier_value: Any, **kwargs: Any) -> Optional[JSONDict]:
        table = self.__format_identifier(kwargs['table'])
        identifier = self.__format_identifier(kwargs['identifier'])
        query = f'SELECT * FROM {table} WHERE {identifier} = %s'
        self.__execute(query, (identifier_value,), **kwargs)
        row = self.__get_data()
        return row if isinstance(row, dict) else None

    def query(self, **kwargs: Any) -> list[JSONDict]:
        if 'params' not in kwargs:
            self.__raise_error('PARAMS_REQUIRED', **kwargs)
        if any(word in kwargs['query'].lower() for word in ['insert', 'update', 'delete']):
            self.__raise_error('READ_ONLY', **kwargs)
        query = kwargs.pop('query')
        params = kwargs.pop('params')
        self.__execute(query, params, **kwargs)
        result = self.__get_data(all=True)
        if isinstance(result, list):
            return list(result)
        return []

    def update(self, **kwargs: Any) -> JSONDict:
        exists = self.__get_existing(**kwargs)
        if not exists:
            self.__raise_error('NOT_EXISTS', **kwargs)
        kwargs['data'] = dict_merger.merge(exists, kwargs['data'], **kwargs)
        query, params = self.__create_update_query(
            kwargs['data'], kwargs['table'], kwargs['identifier']
        )
        self.__execute(query, params, **kwargs)
        super().publish(kwargs['data'], **kwargs)
        return kwargs['data']

    def upsert(self, **kwargs: Any) -> JSONDict:
        exists = self.__get_existing(**kwargs)
        if exists:
            return self.update(**kwargs)
        return self.insert(**kwargs)

    def delete(self, identifier_value: Any, **kwargs: Any) -> None:
        table = self.__format_identifier(kwargs['table'])
        identifier = self.__format_identifier(kwargs['identifier'])
        query = f'DELETE FROM {table} WHERE {identifier} = %s'
        self.__execute(query, (identifier_value,), **kwargs)
        super().publish({kwargs['identifier']: identifier_value}, **kwargs)

    def create_index(self, table_name: str, index_columns: Sequence[str]) -> None:
        table = self.__format_identifier(table_name)
        formatted_columns = [self.__format_identifier(column) for column in index_columns]
        index_name = self.__format_identifier(f'index_{"_".join(index_columns)}')
        statement = f'CREATE INDEX {index_name} ON {table} ({", ".join(formatted_columns)})'
        self.__execute(query=statement, params=None)

    def __create_update_query(self, data: JSONDict, table: str, identifier: str) -> Tuple[str, Tuple[Any, ...]]:
        if identifier not in data:
            raise KeyError(f'identifier "{identifier}" missing from payload for update')
        update_columns = [key for key in data.keys() if key != identifier]
        if not update_columns:
            raise ValueError('no updatable fields supplied for update operation')
        formatted_table = self.__format_identifier(table)
        formatted_identifier = self.__format_identifier(identifier)
        set_clause_parts = []
        for column in update_columns:
            formatted_column = self.__format_identifier(column)
            set_clause_parts.append(f'{formatted_column} = %s')
        set_clause = ', '.join(set_clause_parts)
        params = tuple(data[column] for column in update_columns) + (data[identifier],)
        query = f'UPDATE {formatted_table} SET {set_clause} WHERE {formatted_identifier} = %s'
        return query, params

    def __get_existing(self, **kwargs: Any) -> JSONDict | bool:
        identifier = kwargs['identifier']
        data = kwargs['data']
        if identifier not in data:
            raise KeyError(f'identifier "{identifier}" missing from payload')
        identifier_value = data[identifier]
        table = self.__format_identifier(kwargs['table'])
        identifier_column = self.__format_identifier(identifier)
        query = f'SELECT * FROM {table} WHERE {identifier_column} = %s LIMIT 1'
        self.__execute(query, (identifier_value,), **kwargs)
        result = self.__get_data()
        if isinstance(result, dict):
            return result
        return False

    def __get_data_params(self, **kwargs: Any) -> Tuple[JSONDict, list[str], Tuple[Any, ...]]:
        data = dict(kwargs['data'])
        if not data:
            raise ValueError('no data supplied for insert operation')
        columns = list(data.keys())
        values = tuple(data[column] for column in columns)
        return data, columns, values

    def __get_data(self, **kwargs: Any) -> JSONDict | list[JSONDict] | None:
        if not self.cursor:
            return [] if kwargs.get('all', False) else None
        get = self.cursor.fetchall if kwargs.get('all', False) else self.cursor.fetchone
        try:
            result = get()
        except Exception:
            return [] if kwargs.get('all', False) else None
        if kwargs.get('all', False):
            if isinstance(result, list):
                return list(result)
            return []
        return result if isinstance(result, dict) else None

    def __execute(self, query: str, params: Optional[Sequence[Any]] = None, **kwargs: Any) -> None:
        if not self.cursor or not self.connection:
            raise SQLAdapterException('adapter is not connected')
        try:
            self.__debug(query, params, kwargs.get('debug', False))
            if params is None:
                self.cursor.execute(query)
            else:
                self.cursor.execute(query, params)
            self.commit(kwargs.get('commit', False))
        except Exception as error:
            self.__debug(query, params, True)
            logger.log(level='ERROR', log={'error': error})
            if kwargs.get('rollback'):
                self.connection.rollback()
            raise SQLAdapterException(f'error with execution, check logs - {error}') from error

    def __build_placeholders(self, count: int) -> str:
        if count <= 0:
            raise ValueError('columns must include at least one entry')
        return ', '.join(['%s'] * count)

    def __format_identifier(self, value: str) -> str:
        if not isinstance(value, str) or not self.SAFE_IDENTIFIER.match(value):
            raise ValueError(f'invalid identifier: {value}')
        if self.engine == 'mysql':
            return f'`{value}`'
        return f'"{value}"'

    def __debug(self, query: str, params: Optional[Sequence[Any]], debug: bool = False) -> None:
        if not debug or not self.cursor:
            return
        mogrify = getattr(self.cursor, 'mogrify', None)
        if callable(mogrify):
            try:
                logger.log(level='INFO', log=mogrify(query, params))  # pylint: disable=not-callable
                return
            except Exception:
                pass
        logger.log(level='INFO', log={'query': query, 'params': params})

    def __close_cursor(self) -> None:
        if not self.cursor:
            return
        try:
            self.cursor.close()
        except Exception:
            pass
        self.cursor = None

    def __close_connection(self) -> None:
        if not self.connection:
            return
        try:
            self.connection.close()
        except Exception:
            pass
        self.connection = None

    def __raise_error(self, error_type: str, **kwargs: Any) -> None:
        if error_type == 'PARAMS_REQUIRED':
            raise SQLAdapterException('params kwargs are required to prevent sql inject; send empty dict if not needed')
        if error_type == 'READ_ONLY':
            raise SQLAdapterException(
                'query method is for read-only operations; please use another function for destructive operations'
            )
        if error_type == 'TABLE_WRITE_ONLY':
            raise CreateTableException(
                'create table query-string must start with "create table"'
            )
        if error_type == 'NOT_UNIQUE':
            raise SQLAdapterException(
                f'row already exist with {kwargs["identifier"]} = {kwargs.get("data", {}).get(kwargs["identifier"])}'
            )
        if error_type == 'NOT_EXISTS':
            raise SQLAdapterException(
                f'row does not exist with {kwargs["identifier"]} = {kwargs.get("data", {}).get(kwargs["identifier"])}'
            )
        raise SQLAdapterException(f'Something went wrong and I am not sure how I got here: {error_type}')
