from unittest import mock

import pytest

from daplug_sql import adapter as build_adapter
from daplug_sql.exception import CreateTableException
from tests.integration.postgres import mocks as pg

TABLE_ARGS = {
    'table': 'documents',
    'identifier': 'entity_key'
}


@pytest.fixture(autouse=True)
def reset_table():
    pg.reset_documents_table()
    yield
    pg.reset_documents_table()


@pytest.fixture
def publish_stub(monkeypatch):
    stub = mock.MagicMock()
    monkeypatch.setattr('daplug_core.base_adapter.BaseAdapter.publish', stub)
    return stub


@pytest.fixture
def pg_adapter(publish_stub):
    sql = build_adapter(
        endpoint='127.0.0.1',
        database='daplug',
        user='test',
        password='test',
        port=5432,
        engine='postgres',
    )
    sql.connect()
    sql.install_json_merge()
    yield sql, publish_stub
    sql.close()


def make_document(payload, last_event_at, entity_key='doc-1'):
    return {'entity_key': entity_key, 'payload': payload, 'last_event_at': last_event_at}


def test_atomic_upsert_inserts_and_publishes_written_row(pg_adapter):
    sql, publish = pg_adapter
    payload = {'name': 'Ada', 'preferences': {'food': 'pizza'}}
    result = sql.upsert(data=make_document(payload, 100), **TABLE_ARGS)
    assert result == {'entity_key': 'doc-1', 'payload': payload, 'last_event_at': 100}
    assert publish.call_args.args[0] == result
    stored_payload, stored_guard = pg.fetch_document('doc-1')
    assert stored_payload == payload
    assert stored_guard == 100


def test_atomic_upsert_deep_merges_payload(pg_adapter):
    sql, _ = pg_adapter
    sql.upsert(data=make_document({'name': 'Ada', 'eye_color': 'green', 'preferences': {'food': 'pizza'}}, 100), **TABLE_ARGS)
    result = sql.upsert(
        data=make_document({'name': 'Ada B', 'preferences': {'music': 'jazz'}}, 200),
        merge_columns=['payload'],
        **TABLE_ARGS,
    )
    assert result['payload'] == {
        'name': 'Ada B',
        'eye_color': 'green',
        'preferences': {'food': 'pizza', 'music': 'jazz'},
    }


def test_atomic_upsert_strip_paths_prunes_stale_keys(pg_adapter):
    sql, _ = pg_adapter
    sql.upsert(data=make_document({'name': 'Ada', 'eye_color': 'green', 'preferences': {'food': 'pizza', 'music': 'jazz'}}, 100), **TABLE_ARGS)
    result = sql.upsert(
        data=make_document({'name': 'Ada'}, 200),
        merge_columns=['payload'],
        strip_paths={'payload': ['eye_color', 'preferences.music']},
        **TABLE_ARGS,
    )
    assert result['payload'] == {'name': 'Ada', 'preferences': {'food': 'pizza'}}


def test_atomic_upsert_guard_blocks_stale_then_accepts_newer(pg_adapter):
    sql, publish = pg_adapter
    sql.upsert(data=make_document({'name': 'current'}, 200), **TABLE_ARGS)
    publish.reset_mock()
    blocked = sql.upsert(
        data=make_document({'name': 'stale'}, 100),
        guard_column='last_event_at',
        **TABLE_ARGS,
    )
    assert blocked is None
    publish.assert_not_called()
    assert pg.fetch_document('doc-1') == ({'name': 'current'}, 200)
    accepted = sql.upsert(
        data=make_document({'name': 'newer'}, 300),
        guard_column='last_event_at',
        **TABLE_ARGS,
    )
    assert accepted['last_event_at'] == 300
    assert pg.fetch_document('doc-1') == ({'name': 'newer'}, 300)


def test_legacy_upsert_path_still_works(pg_adapter):
    sql, _ = pg_adapter
    result = sql.upsert(data=make_document({'name': 'legacy'}, 100, 'doc-legacy'), atomic=False, **TABLE_ARGS)
    assert result['entity_key'] == 'doc-legacy'
    assert pg.fetch_document('doc-legacy')[0] == {'name': 'legacy'}


def test_create_table_validates_and_creates(pg_adapter):
    sql, _ = pg_adapter
    with pytest.raises(CreateTableException):
        sql.create_table(query='DROP TABLE documents')
    sql.create_table(query='CREATE TABLE IF NOT EXISTS scratch (id VARCHAR(32) PRIMARY KEY, payload JSONB)')
    sql.insert(data={'id': 'row-1', 'payload': {'ok': True}}, table='scratch', identifier='id')
    assert sql.get('row-1', table='scratch', identifier='id')['payload'] == {'ok': True}
