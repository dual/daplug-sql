from unittest import mock

import pytest

from daplug_sql.sql_connector import SQLConnector
from tests.unit.mocks.adapters import ConnectorHost


@pytest.fixture
def postgres_connector():
    host = ConnectorHost(autocommit=True)
    return SQLConnector(host)


def test_postgres_connect_initializes_connection(postgres_connector):
    fake_connection = mock.MagicMock(closed=0)
    with mock.patch('daplug_sql.sql_connector.psycopg2.connect', return_value=fake_connection) as connect:
        connection = postgres_connector.connect()
    connect.assert_called_once_with(
        dbname='app', host='db.local', port=5432, user='svc', password='secret'
    )
    fake_connection.set_session.assert_called_once_with(autocommit=True)
    assert connection is fake_connection


def test_postgres_connect_without_autocommit_skips_session():
    host = ConnectorHost(autocommit=False)
    connector = SQLConnector(host)
    fake_connection = mock.MagicMock(closed=0)
    with mock.patch('daplug_sql.sql_connector.psycopg2.connect', return_value=fake_connection):
        connector.connect()
    fake_connection.set_session.assert_not_called()


def test_postgres_connect_reuses_existing_connection(postgres_connector):
    postgres_connector.connection = mock.MagicMock(closed=0)
    with mock.patch('daplug_sql.sql_connector.psycopg2.connect') as connect:
        connection = postgres_connector.connect()
    connect.assert_not_called()
    assert connection is postgres_connector.connection


def test_postgres_cursor_uses_real_dict_cursor(postgres_connector):
    fake_connection = mock.MagicMock(closed=0)
    fake_cursor = mock.MagicMock()
    fake_connection.cursor.return_value = fake_cursor
    with mock.patch('daplug_sql.sql_connector.psycopg2.connect', return_value=fake_connection):
        cursor = postgres_connector.cursor()
    fake_connection.cursor.assert_called_once()
    _, kwargs = fake_connection.cursor.call_args
    assert 'cursor_factory' in kwargs
    assert cursor is fake_cursor


def test_mysql_connect_initializes_connection():
    host = ConnectorHost(engine='mysql', autocommit=True, port=3306)
    connector = SQLConnector(host)
    fake_connection = mock.MagicMock()
    fake_connection.is_connected.return_value = False
    with mock.patch('daplug_sql.sql_connector.mysql.connector.connect', return_value=fake_connection) as connect:
        connection = connector.connect()
    connect.assert_called_once_with(
        host='db.local',
        user='svc',
        password='secret',
        database='app',
        port=3306,
        charset='utf8mb4',
    )
    assert fake_connection.autocommit is True
    assert connection is fake_connection


def test_mysql_connect_reuses_when_connected():
    host = ConnectorHost(engine='mysql', autocommit=False)
    connector = SQLConnector(host)
    fake_connection = mock.MagicMock()
    fake_connection.is_connected.return_value = True
    connector.connection = fake_connection
    with mock.patch('daplug_sql.sql_connector.mysql.connector.connect') as connect:
        connection = connector.connect()
    connect.assert_not_called()
    assert fake_connection.autocommit is False
    assert connection is fake_connection


def test_mysql_cursor_returns_dictionary_cursor():
    host = ConnectorHost(engine='mysql')
    connector = SQLConnector(host)
    fake_connection = mock.MagicMock()
    fake_cursor = mock.MagicMock()
    fake_connection.cursor.return_value = fake_cursor
    fake_connection.is_connected.return_value = False
    with mock.patch('daplug_sql.sql_connector.mysql.connector.connect', return_value=fake_connection):
        cursor = connector.cursor()
    fake_connection.cursor.assert_called_once_with(dictionary=True)
    assert cursor is fake_cursor
