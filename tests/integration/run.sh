#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f tests/integration/docker-compose.yml"

cleanup() {
  $COMPOSE down -v >/dev/null 2>&1 || true
}

wait_for_postgres() {
  python - <<'PY'
import time
import os
import psycopg2

def ready():
    conn = psycopg2.connect(
        host=os.getenv('SQL_POSTGRES_HOST', '127.0.0.1'),
        port=int(os.getenv('SQL_POSTGRES_PORT', '5432')),
        dbname=os.getenv('SQL_POSTGRES_DB', 'daplug'),
        user=os.getenv('SQL_POSTGRES_USER', 'test'),
        password=os.getenv('SQL_POSTGRES_PASSWORD', 'test'),
    )
    conn.close()

end = time.time() + 60
while True:
    try:
        ready()
        break
    except Exception as exc:  # pylint: disable=broad-except
        if time.time() > end:
            raise SystemExit(f'Postgres not ready: {exc}')
        time.sleep(1)
PY
}

wait_for_mysql() {
  python - <<'PY'
import time
import os
import mysql.connector

def ready():
    conn = mysql.connector.connect(
        host=os.getenv('SQL_MYSQL_HOST', '127.0.0.1'),
        port=int(os.getenv('SQL_MYSQL_PORT', '3306')),
        database=os.getenv('SQL_MYSQL_DB', 'daplug'),
        user=os.getenv('SQL_MYSQL_USER', 'test'),
        password=os.getenv('SQL_MYSQL_PASSWORD', 'test'),
    )
    conn.close()

end = time.time() + 60
while True:
    try:
        ready()
        break
    except Exception as exc:  # pylint: disable=broad-except
        if time.time() > end:
            raise SystemExit(f'MySQL not ready: {exc}')
        time.sleep(1)
PY
}

trap cleanup EXIT

$COMPOSE up -d
wait_for_postgres
wait_for_mysql
pytest tests/integration
