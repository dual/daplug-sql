from unittest import mock

import pytest

import daplug_sql.sql_connection as sc


@pytest.fixture(autouse=True)
def reset_cache():
    sc._connection_cache.clear()
    yield
    sc._connection_cache.clear()


def build_adapter(**overrides):
    defaults = {
        'endpoint': 'db.local',
        'database': 'app',
        'user': 'svc',
        'password': 'pw',
        'port': 5432,
        'engine': overrides.get('engine', 'postgres')
    }
    defaults.update(overrides)
    adapter = type('Adapter', (), defaults)()
    return adapter


def test_sql_connection_caches_connector(monkeypatch):
    created = []

    class StubConnector:
        def __init__(self, adapter):
            self.adapter = adapter
            self.connection = mock.MagicMock(closed=0)
            created.append(self)

        def connect(self):
            return self.connection

    monkeypatch.setattr(sc, 'SQLConnector', StubConnector)

    @sc.sql_connection
    def connect_method(adapter, connector):
        return connector

    adapter = build_adapter()
    first = connect_method(adapter)
    second = connect_method(adapter)
    assert first is second
    assert len(created) == 1


def test_sql_connection_recreates_when_closed(monkeypatch):
    created = []

    class StubConnector:
        def __init__(self, adapter):
            self.connection = mock.MagicMock(closed=0)
            created.append(self)

        def connect(self):
            return self.connection

    monkeypatch.setattr(sc, 'SQLConnector', StubConnector)

    @sc.sql_connection
    def connect_method(adapter, connector):
        return connector

    adapter = build_adapter()
    connector = connect_method(adapter)
    connector.connection.closed = 1
    new_connector = connect_method(adapter)
    assert connector is not new_connector
    assert len(created) == 2


def test_sql_connection_cleanup_closes_and_removes(monkeypatch):
    closed = []

    class StubConnector:
        def __init__(self, adapter):
            self.connection = mock.MagicMock()
            self.connection.close.side_effect = lambda: closed.append(True)

        def connect(self):
            return self.connection

    monkeypatch.setattr(sc, 'SQLConnector', StubConnector)

    @sc.sql_connection
    def connect_method(adapter, connector):
        return connector

    @sc.sql_connection_cleanup
    def close_method(adapter):
        return 'done'

    adapter = build_adapter()
    connect_method(adapter)
    result = close_method(adapter)
    assert result == 'done'
    assert not sc._connection_cache
    assert closed == [True]


def test_close_connectors_for_all_entries(monkeypatch):
    connector = mock.MagicMock()
    connector.connection = mock.MagicMock()
    sc._connection_cache[('k',)] = connector
    connection = connector.connection
    sc._close_connectors_for()
    connection.close.assert_called_once()
    assert not sc._connection_cache


def test_is_connection_closed_variants():
    assert sc._is_connection_closed(None) is True
    assert sc._is_connection_closed(mock.Mock(closed=1)) is True
    assert sc._is_connection_closed(mock.Mock(closed=0)) is False
    assert sc._is_connection_closed(mock.Mock(open=0)) is True

    mysql_like = mock.Mock(open=None, closed=None)
    mysql_like.is_connected.return_value = False
    assert sc._is_connection_closed(mysql_like) is True
    mysql_like.is_connected.return_value = True
    assert sc._is_connection_closed(mysql_like) is False
