import json

import pytest
from psycopg2.extras import Json

from daplug_sql.params import adapt_sequence, adapt_value
from daplug_sql.upsert_builder import UpsertBuilder


def build_kwargs(**overrides):
    kwargs = {
        'data': {'entity_key': 'abc', 'payload': {'name': 'Ada'}, 'last_event_at': 100},
        'table': 'documents',
        'identifier': 'entity_key',
    }
    kwargs.update(overrides)
    return kwargs


def test_postgres_plain_upsert():
    query, params = UpsertBuilder('postgres', **build_kwargs()).build()
    assert query == (
        'INSERT INTO "documents" AS existing ("entity_key", "payload", "last_event_at") '
        'VALUES (%s, %s, %s) '
        'ON CONFLICT ("entity_key") DO UPDATE SET '
        '"payload" = EXCLUDED."payload", "last_event_at" = EXCLUDED."last_event_at" '
        'RETURNING *'
    )
    assert params[0] == 'abc'
    assert isinstance(params[1], Json)
    assert params[2] == 100


def test_postgres_merge_strip_and_guard():
    kwargs = build_kwargs(
        merge_columns=['payload'],
        strip_paths={'payload': ['eye_color', 'preferences.music']},
        guard_column='last_event_at',
    )
    query, params = UpsertBuilder('postgres', **kwargs).build()
    assert '"payload" = ((daplug_json_merge(existing."payload", EXCLUDED."payload")) #- %s) #- %s' in query
    assert 'WHERE existing."last_event_at" IS NULL OR EXCLUDED."last_event_at" >= existing."last_event_at"' in query
    assert query.endswith('RETURNING *')
    assert params[-2:] == (['eye_color'], ['preferences', 'music'])


def test_postgres_identifier_only_payload_does_nothing_on_conflict():
    kwargs = build_kwargs(data={'entity_key': 'abc'})
    query, params = UpsertBuilder('postgres', **kwargs).build()
    assert 'ON CONFLICT ("entity_key") DO NOTHING' in query
    assert params == ('abc',)


def test_mysql_merge_strip_and_guard():
    kwargs = build_kwargs(
        merge_columns=['payload'],
        strip_paths={'payload': ['eye_color']},
        guard_column='last_event_at',
    )
    query, params = UpsertBuilder('mysql', **kwargs).build()
    assert query.startswith('INSERT INTO `documents` (`entity_key`, `payload`, `last_event_at`) VALUES (%s, %s, %s) AS new_values')
    assert 'ON DUPLICATE KEY UPDATE' in query
    assert (
        '`payload` = IF(`documents`.`last_event_at` IS NULL OR new_values.`last_event_at` >= `documents`.`last_event_at`, '
        'JSON_REMOVE(JSON_MERGE_PATCH(COALESCE(`documents`.`payload`, JSON_OBJECT()), new_values.`payload`), %s), `documents`.`payload`)'
    ) in query
    assert params[1] == json.dumps({'name': 'Ada'})
    assert params[-1] == '$."eye_color"'


def test_mysql_identifier_only_payload_is_noop_on_conflict():
    kwargs = build_kwargs(data={'entity_key': 'abc'})
    query, _ = UpsertBuilder('mysql', **kwargs).build()
    assert query.endswith('ON DUPLICATE KEY UPDATE `entity_key` = `documents`.`entity_key`')


def test_build_validations():
    with pytest.raises(ValueError):
        UpsertBuilder('postgres', **build_kwargs(data={})).build()
    with pytest.raises(KeyError):
        UpsertBuilder('postgres', **build_kwargs(data={'payload': {}})).build()
    with pytest.raises(ValueError):
        UpsertBuilder('postgres', **build_kwargs(table='bad table')).build()


def test_adapt_value_by_engine():
    assert adapt_value('postgres', 5) == 5
    assert isinstance(adapt_value('postgres', {'a': 1}), Json)
    assert isinstance(adapt_value('postgres', [1, 2]), Json)
    assert adapt_value('mysql', {'a': 1}) == '{"a": 1}'
    assert adapt_sequence('mysql', ('x', [1])) == ('x', '[1]')
