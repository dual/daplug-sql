from unittest import mock

import pytest

from daplug_sql.adapter import SQLAdapter
from tests.integration.postgres import mocks as pg

TABLE_ARGS = {
    'table': 'items', 
    'identifier': 'external_id'
}

@pytest.fixture(autouse=True)
def reset_table():
    pg.reset_items_table()
    yield
    pg.reset_items_table()


@pytest.fixture
def publish_stub(monkeypatch):
    stub = mock.MagicMock()
    monkeypatch.setattr('daplug_core.base_adapter.BaseAdapter.publish', stub)
    return stub


@pytest.fixture
def pg_adapter(publish_stub):
    adapter = SQLAdapter(
        endpoint='127.0.0.1',
        database='daplug',
        user='test',
        password='test',
        port=5432,
        engine='postgres',
    )
    adapter.connect()
    yield adapter, publish_stub
    adapter.close()


def make_data(external_id='pg-row', name='alpha', value=1):
    return {'external_id': external_id, 'name': name, 'value': value}


def assert_publish_called(publish, data):
    args, kwargs = publish.call_args
    assert args[0] == data
    assert kwargs['table'] == 'items'
    assert kwargs['identifier'] == 'external_id'
    assert kwargs['data'] == data


def test_connect_and_close(pg_adapter):
    adapter, _ = pg_adapter
    assert adapter.connection
    adapter.close()
    assert adapter.connection is None


def test_connect_with_invalid_credentials():
    adapter = SQLAdapter(
        endpoint='127.0.0.1',
        database='daplug',
        user='test',
        password='bad',
        port=55432,
        engine='postgres',
    )
    with pytest.raises(Exception):
        adapter.connect()


def test_commit_toggle(pg_adapter):
    adapter, _ = pg_adapter
    adapter.connection = mock.MagicMock()
    adapter.commit(True)
    adapter.connection.commit.assert_called_once()
    adapter.connection.commit.reset_mock()
    adapter.commit(False)
    adapter.connection.commit.assert_not_called()


def test_create_and_duplicate(pg_adapter):
    adapter, publish = pg_adapter
    data = make_data('pg-create', 'alpha', 1)
    adapter.create(data=data, **TABLE_ARGS)
    assert_publish_called(publish, data)
    assert pg.fetch_item('pg-create')[1:] == ('alpha', 1)
    with pytest.raises(Exception):
        adapter.create(data=data, **TABLE_ARGS)


def test_insert_positive_and_duplicate(pg_adapter):
    adapter, publish = pg_adapter
    data = make_data('pg-insert', 'beta', 2)
    adapter.insert(data=data, **TABLE_ARGS)
    assert_publish_called(publish, data)
    with pytest.raises(Exception):
        adapter.insert(data=data, **TABLE_ARGS)


def test_update_success_and_missing(pg_adapter):
    adapter, publish = pg_adapter
    pg.insert_item('pg-update', 'before', 1)
    updated = make_data('pg-update', 'after', 9)
    adapter.update(data=updated, **TABLE_ARGS)
    assert_publish_called(publish, updated)
    assert pg.fetch_item('pg-update')[1:] == ('after', 9)
    with pytest.raises(Exception):
        adapter.update(data=make_data('pg-missing', 'x', 1), **TABLE_ARGS)


def test_upsert_update_and_insert(pg_adapter):
    adapter, _ = pg_adapter
    pg.insert_item('pg-upsert', 'keep', 1)
    result_update = adapter.upsert(data=make_data('pg-upsert', 'changed', 5), **TABLE_ARGS)
    assert result_update['name'] == 'changed'
    result_insert = adapter.upsert(data=make_data('pg-upsert-new', 'new', 7), **TABLE_ARGS)
    assert pg.fetch_item('pg-upsert-new')[1:] == ('new', 7)
    assert result_insert['external_id'] == 'pg-upsert-new'


def test_read_and_get(pg_adapter):
    adapter, _ = pg_adapter
    pg.insert_item('pg-read', 'row', 3)
    assert adapter.read('pg-read', **TABLE_ARGS) == {'external_id': 'pg-read', 'name': 'row', 'value': 3}
    assert adapter.get('missing', **TABLE_ARGS) is None


def test_query_enforces_params(pg_adapter):
    adapter, _ = pg_adapter
    pg.insert_item('pg-query', 'row', 4)
    rows = adapter.query(
        query='SELECT * FROM items WHERE external_id = %(external_id)s',
        params={'external_id': 'pg-query'},
    )
    assert rows == [{'external_id': 'pg-query', 'name': 'row', 'value': 4}]
    with pytest.raises(Exception):
        adapter.query(query='SELECT 1')
    with pytest.raises(Exception):
        adapter.query(query='DELETE FROM items', params={})


def test_delete_existing_and_missing(pg_adapter):
    adapter, publish = pg_adapter
    pg.insert_item('pg-delete', 'row', 5)
    adapter.delete('pg-delete', **TABLE_ARGS)
    assert publish.call_count == 1
    assert pg.fetch_item('pg-delete') is None
    adapter.delete('pg-missing', **TABLE_ARGS)
    assert publish.call_count == 2


def test_create_index_success_and_failure(pg_adapter):
    adapter, _ = pg_adapter
    adapter.create_index('items', ['name'])
    assert pg.index_exists('index_name')
    with pytest.raises(Exception):
        adapter.create_index('items', ['unknown_column'])


def test_close_idempotent(pg_adapter):
    adapter, _ = pg_adapter
    adapter.close()
    adapter.close()
