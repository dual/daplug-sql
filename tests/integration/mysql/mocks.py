import mysql.connector

SETTINGS = {
    'host': '127.0.0.1',
    'port': 53306,
    'user': 'test',
    'password': 'test',
    'database': 'daplug',
}


def connection():
    return mysql.connector.connect(**SETTINGS)


def reset_items_table():
    conn = connection()
    cur = conn.cursor()
    cur.execute('DROP TABLE IF EXISTS items')
    cur.execute(
        'CREATE TABLE items ('
        ' external_id VARCHAR(64) PRIMARY KEY,'
        ' name VARCHAR(255) NOT NULL,'
        ' value INT NOT NULL'
        ') ENGINE=InnoDB'
    )
    conn.commit()
    cur.close()
    conn.close()


def insert_item(external_id, name, value):
    conn = connection()
    cur = conn.cursor()
    cur.execute(
        'INSERT INTO items (external_id, name, value) VALUES (%s, %s, %s)',
        (external_id, name, value),
    )
    conn.commit()
    cur.close()
    conn.close()


def fetch_item(external_id):
    conn = connection()
    cur = conn.cursor()
    cur.execute('SELECT external_id, name, value FROM items WHERE external_id = %s', (external_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row


def fetch_all_items():
    conn = connection()
    cur = conn.cursor()
    cur.execute('SELECT external_id, name, value FROM items ORDER BY external_id')
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows


def index_exists(index_name):
    conn = connection()
    cur = conn.cursor(dictionary=True)
    cur.execute('SHOW INDEX FROM items WHERE Key_name = %s', (index_name,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists
