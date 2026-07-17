from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from .param_adapter import ParamAdapter
from .types import JSONDict


class UpsertBuilder:

    SAFE_IDENTIFIER = re.compile(r'^[A-Za-z_]\w*$')

    POSTGRES_JSON_MERGE_FUNCTION = '''
CREATE OR REPLACE FUNCTION daplug_json_merge(existing jsonb, incoming jsonb)
RETURNS jsonb
LANGUAGE plpgsql
IMMUTABLE
AS $daplug$
BEGIN
    IF existing IS NULL THEN
        RETURN incoming;
    END IF;
    IF incoming IS NULL THEN
        RETURN existing;
    END IF;
    IF jsonb_typeof(existing) = 'object' AND jsonb_typeof(incoming) = 'object' THEN
        RETURN COALESCE(
            (
                SELECT jsonb_object_agg(
                    COALESCE(existing_field.key, incoming_field.key),
                    CASE
                        WHEN existing_field.value IS NULL THEN incoming_field.value
                        WHEN incoming_field.value IS NULL THEN existing_field.value
                        ELSE daplug_json_merge(existing_field.value, incoming_field.value)
                    END
                )
                FROM jsonb_each(existing) AS existing_field
                FULL JOIN jsonb_each(incoming) AS incoming_field
                    ON existing_field.key = incoming_field.key
            ),
            '{}'::jsonb
        );
    END IF;
    RETURN incoming;
END;
$daplug$;
'''

    def __init__(self, engine: str, **kwargs: Any) -> None:
        self.engine: str = engine
        self.data: JSONDict = kwargs['data']
        self.table: str = kwargs['table']
        self.identifier: str = kwargs['identifier']
        self.merge_columns: List[str] = list(kwargs.get('merge_columns') or [])
        self.strip_paths: Dict[str, List[str]] = {
            column: list(paths) for column, paths in dict(kwargs.get('strip_paths') or {}).items()
        }
        self.guard_column: Optional[str] = kwargs.get('guard_column')
        self.columns: List[str] = list(self.data.keys())

    def build(self) -> Tuple[str, Tuple[Any, ...]]:
        if not self.columns:
            raise ValueError('no data supplied for upsert operation')
        if self.identifier not in self.data:
            raise KeyError(f'identifier "{self.identifier}" missing from payload for upsert')
        if self.engine == 'mysql':
            return self.__build_mysql()
        return self.__build_postgres()

    def __build_postgres(self) -> Tuple[str, Tuple[Any, ...]]:
        set_parts: List[str] = []
        set_params: List[Any] = []
        for column in self.__update_columns():
            expression, params = self.__postgres_expression(column)
            set_parts.append(f'{self.__format(column)} = {expression}')
            set_params.extend(params)
        conflict_action = f'DO UPDATE SET {", ".join(set_parts)}' if set_parts else 'DO NOTHING'
        query = (
            f'INSERT INTO {self.__format(self.table)} AS existing ({self.__column_clause()}) '
            f'VALUES ({self.__placeholder_clause()}) '
            f'ON CONFLICT ({self.__format(self.identifier)}) {conflict_action}'
        )
        if self.guard_column and set_parts:
            guard = self.__format(self.guard_column)
            query += f' WHERE existing.{guard} IS NULL OR EXCLUDED.{guard} >= existing.{guard}'
        query += ' RETURNING *'
        return query, self.__insert_params() + tuple(set_params)

    def __postgres_expression(self, column: str) -> Tuple[str, List[Any]]:
        formatted = self.__format(column)
        expression = f'EXCLUDED.{formatted}'
        if column in self.merge_columns:
            expression = f'daplug_json_merge(existing.{formatted}, EXCLUDED.{formatted})'
        params: List[Any] = []
        for path in self.strip_paths.get(column, []):
            expression = f'({expression}) #- %s'
            params.append(path.split('.'))
        return expression, params

    def __build_mysql(self) -> Tuple[str, Tuple[Any, ...]]:
        set_parts: List[str] = []
        set_params: List[Any] = []
        table = self.__format(self.table)
        for column in self.__update_columns():
            expression, params = self.__mysql_expression(column)
            if self.guard_column:
                guard = f'{table}.{self.__format(self.guard_column)}'
                expression = f'IF({guard} IS NULL OR new_values.{self.__format(self.guard_column)} >= {guard}, {expression}, {table}.{self.__format(column)})'
            set_parts.append(f'{self.__format(column)} = {expression}')
            set_params.extend(params)
        identifier = self.__format(self.identifier)
        update_clause = ', '.join(set_parts) if set_parts else f'{identifier} = {table}.{identifier}'
        query = (
            f'INSERT INTO {self.__format(self.table)} ({self.__column_clause()}) '
            f'VALUES ({self.__placeholder_clause()}) AS new_values '
            f'ON DUPLICATE KEY UPDATE {update_clause}'
        )
        return query, self.__insert_params() + tuple(set_params)

    def __mysql_expression(self, column: str) -> Tuple[str, List[Any]]:
        formatted = self.__format(column)
        expression = f'new_values.{formatted}'
        if column in self.merge_columns:
            existing = f'{self.__format(self.table)}.{formatted}'
            expression = f'JSON_MERGE_PATCH(COALESCE({existing}, JSON_OBJECT()), new_values.{formatted})'
        params: List[Any] = []
        for path in self.strip_paths.get(column, []):
            expression = f'JSON_REMOVE({expression}, %s)'
            params.append(self.__mysql_path(path))
        return expression, params

    def __mysql_path(self, path: str) -> str:
        segments = []
        for segment in path.split('.'):
            escaped = segment.replace('"', '\\"')
            segments.append(f'"{escaped}"')
        return '$.' + '.'.join(segments)

    def __update_columns(self) -> List[str]:
        return [column for column in self.columns if column != self.identifier]

    def __column_clause(self) -> str:
        return ', '.join(self.__format(column) for column in self.columns)

    def __placeholder_clause(self) -> str:
        return ', '.join(['%s'] * len(self.columns))

    def __insert_params(self) -> Tuple[Any, ...]:
        return ParamAdapter(self.engine).sequence(tuple(self.data[column] for column in self.columns))

    def __format(self, value: str) -> str:
        if not isinstance(value, str) or not self.SAFE_IDENTIFIER.match(value):
            raise ValueError(f'invalid identifier: {value}')
        if self.engine == 'mysql':
            return f'`{value}`'
        return f'"{value}"'
