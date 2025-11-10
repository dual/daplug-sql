from unittest import mock

import pytest

import daplug_sql.sql_connection as sc
from daplug_sql.adapter import SQLAdapter


@pytest.fixture(autouse=True)
def reset_connection_cache():
    sc._connection_cache.clear()
    yield
    sc._connection_cache.clear()


@pytest.fixture
def publish_mock(monkeypatch):
    mock_publish = mock.MagicMock()
    monkeypatch.setattr('daplug_core.base_adapter.BaseAdapter.publish', mock_publish)
    return mock_publish


@pytest.fixture
def adapter(monkeypatch, publish_mock):
    inst = SQLAdapter(
        endpoint='db.local',
        database='app',
        user='svc',
        password='pw'
    )
    inst.connection = mock.MagicMock()
    inst.cursor = mock.MagicMock()
    inst.cursor.fetchone.return_value = {'id': 1}
    inst.cursor.fetchall.return_value = [{'id': 1}]
    return inst


def test_connect_assigns_connection_and_cursor(monkeypatch):
    connection = mock.MagicMock()
    cursor = mock.MagicMock()

    class StubConnector:
        def __init__(self, obj):
            self.obj = obj

        def connect(self):
            return connection

        def cursor(self):
            return cursor

    monkeypatch.setattr(sc, 'SQLConnector', StubConnector)

    inst = SQLAdapter(endpoint='db.local', database='app', user='svc', password='pw')
    inst.connect()
    assert inst.connection is connection
    assert inst.cursor is cursor


def test_commit_controls_commit_call(adapter):
    adapter.commit(commit=True)
    adapter.connection.commit.assert_called_once()
    adapter.connection.commit.reset_mock()
    adapter.commit(commit=False)
    adapter.connection.commit.assert_not_called()


def test_insert_executes_and_publishes(adapter, publish_mock, monkeypatch):
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_existing', lambda self, **_: False)
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_data_params', lambda self, **_: ({'id': 1}, ['id'], (1,)))
    adapter.insert(table='items', identifier='id', data={'id': 1})
    adapter.cursor.execute.assert_called_once()
    publish_mock.assert_called_once_with({'id': 1}, table='items', identifier='id', data={'id': 1})


def test_insert_raises_on_duplicate(adapter, monkeypatch):
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_existing', lambda self, **_: {'id': 1})
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_data_params', lambda self, **_: ({'id': 1}, ['id'], (1,)))
    with pytest.raises(Exception) as exc:
        adapter.insert(table='items', identifier='id', data={'id': 1})
    assert 'row already exist' in str(exc.value)


def test_update_merges_and_executes(adapter, publish_mock, monkeypatch):
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_existing', lambda self, **_: {'id': 1, 'name': 'old'})

    def fake_merge(existing, new, **kwargs):
        return {'id': 1, 'name': new['name']}

    monkeypatch.setattr('daplug_sql.adapter.dict_merger.merge', fake_merge)
    adapter.update(table='items', identifier='id', data={'id': 1, 'name': 'new'})
    adapter.cursor.execute.assert_called()
    publish_mock.assert_called()


def test_update_raises_when_missing(adapter, monkeypatch):
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_existing', lambda self, **_: False)
    with pytest.raises(Exception) as exc:
        adapter.update(table='items', identifier='id', data={'id': 1})
    assert 'does not exist' in str(exc.value)


def test_upsert_paths(adapter, monkeypatch):
    updater = mock.MagicMock(return_value='updated')
    inserter = mock.MagicMock(return_value='inserted')
    monkeypatch.setattr(SQLAdapter, 'update', updater)
    monkeypatch.setattr(SQLAdapter, 'insert', inserter)
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_existing', lambda self, **_: True)
    assert adapter.upsert(table='items', identifier='id', data={'id': 1}) == 'updated'
    monkeypatch.setattr(SQLAdapter, '_SQLAdapter__get_existing', lambda self, **_: False)
    assert adapter.upsert(table='items', identifier='id', data={'id': 1}) == 'inserted'


def test_get_returns_row(adapter):
    row = adapter.get(1, table='items', identifier='id')
    adapter.cursor.execute.assert_called_once()
    assert row == {'id': 1}


def test_query_validation(adapter):
    with pytest.raises(Exception):
        adapter.query(query='select 1')
    with pytest.raises(Exception):
        adapter.query(query='delete * from x', params={})


def test_query_returns_rows(adapter):
    rows = adapter.query(query='select * from items', params={})
    adapter.cursor.execute.assert_called_once()
    assert rows == [{'id': 1}]


def test_delete_executes_and_publishes(adapter, publish_mock):
    adapter.delete(1, table='items', identifier='id')
    adapter.cursor.execute.assert_called_once()
    publish_mock.assert_called()


def test_create_index_formats_identifiers(adapter):
    adapter.create_index('items', ['col'])
    adapter.cursor.execute.assert_called_once()


def test_create_update_query_validations(adapter):
    with pytest.raises(KeyError):
        adapter._SQLAdapter__create_update_query({}, 'items', 'id')
    with pytest.raises(ValueError):
        adapter._SQLAdapter__create_update_query({'id': 1}, 'items', 'id')

    query, params = adapter._SQLAdapter__create_update_query({'id': 1, 'name': 'n'}, 'items', 'id')
    assert 'SET' in query
    assert params[-1] == 1


def test_get_existing_behaviors(adapter):
    adapter.cursor.fetchone.return_value = {'id': 1}
    assert adapter._SQLAdapter__get_existing(table='items', identifier='id', data={'id': 1}) == {'id': 1}
    adapter.cursor.fetchone.return_value = None
    assert adapter._SQLAdapter__get_existing(table='items', identifier='id', data={'id': 1}) is False
    with pytest.raises(KeyError):
        adapter._SQLAdapter__get_existing(table='items', identifier='id', data={})


def test_get_data_params_validations(adapter):
    with pytest.raises(ValueError):
        adapter._SQLAdapter__get_data_params(table='items', identifier='id', data={})
    data, cols, values = adapter._SQLAdapter__get_data_params(table='items', identifier='id', data={'id': 1, 'name': 'a'})
    assert cols == ['id', 'name']
    assert values == (1, 'a')


def test_get_data_handles_all_and_single(adapter):
    adapter.cursor.fetchone.return_value = {'id': 1}
    assert adapter._SQLAdapter__get_data() == {'id': 1}
    adapter.cursor.fetchall.side_effect = Exception('boom')
    assert adapter._SQLAdapter__get_data(all=True) == []


def test_execute_handles_errors(adapter):
    adapter.cursor.execute.side_effect = RuntimeError('fail')
    with pytest.raises(Exception):
        adapter._SQLAdapter__execute('SELECT 1', None, rollback=True)
    adapter.connection.rollback.assert_called_once()


def test_build_placeholders_and_format_identifier(adapter):
    with pytest.raises(ValueError):
        adapter._SQLAdapter__build_placeholders(0)
    assert adapter._SQLAdapter__build_placeholders(2) == '%s, %s'
    assert adapter._SQLAdapter__format_identifier('abc') == '"abc"'
    adapter.engine = 'mysql'
    assert adapter._SQLAdapter__format_identifier('abc') == '`abc`'
    with pytest.raises(ValueError):
        adapter._SQLAdapter__format_identifier('1bad')


def test_raise_error_paths(adapter):
    with pytest.raises(Exception):
        adapter._SQLAdapter__raise_error('PARAMS_REQUIRED')
    with pytest.raises(Exception):
        adapter._SQLAdapter__raise_error('READ_ONLY')
    with pytest.raises(Exception):
        adapter._SQLAdapter__raise_error('TABLE_WRITE_ONLY')
    with pytest.raises(Exception):
        adapter._SQLAdapter__raise_error('NOT_UNIQUE', identifier='id', data={'id': 1})
    with pytest.raises(Exception):
        adapter._SQLAdapter__raise_error('NOT_EXISTS', identifier='id', data={'id': 1})
    with pytest.raises(Exception):
        adapter._SQLAdapter__raise_error('UNKNOWN')


def test_close_closes_cursor_and_connection(adapter, monkeypatch):
    close_connectors = mock.MagicMock()
    monkeypatch.setattr(sc, '_close_connectors_for', close_connectors)
    cursor = adapter.cursor
    connection = adapter.connection
    adapter.close()
    cursor.close.assert_called_once()
    connection.close.assert_called_once()
    assert adapter.cursor is None
    assert adapter.connection is None
    close_connectors.assert_called_once_with(adapter)
