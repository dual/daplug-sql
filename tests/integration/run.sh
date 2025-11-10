#!/usr/bin/env bash
set -euo pipefail

COMPOSE="docker compose -f tests/integration/docker-compose.yml"

cleanup() {
  $COMPOSE down -v >/dev/null 2>&1 || true
}

wait_for_postgres() {
  python - <<'PY'
import time
import psycopg2

def ready():
    conn = psycopg2.connect(
        host='127.0.0.1',
        port=55432,
        dbname='daplug',
        user='test',
        password='test',
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
import mysql.connector

def ready():
    conn = mysql.connector.connect(
        host='127.0.0.1',
        port=53306,
        database='daplug',
        user='test',
        password='test',
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
