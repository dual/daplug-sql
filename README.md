# 🔗 daplug-sql (da•plug)

> **Schema-Driven SQL Normalization & Event Publishing for Python**

[![CircleCI](https://circleci.com/gh/dual/daplug-sql.svg?style=shield)](https://circleci.com/gh/dual/daplug-sql)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-sql&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=dual_daplug-sql)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-sql&metric=bugs)](https://sonarcloud.io/summary/new_code?id=dual_daplug-sql)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=dual_daplug-sql&metric=coverage)](https://sonarcloud.io/summary/new_code?id=dual_daplug-sql)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/downloads/)
[![PyPI package](https://img.shields.io/pypi/v/daplug-sql?color=blue&label=pypi%20package)](https://pypi.org/project/daplug-sql/)
[![License](https://img.shields.io/badge/license-apache%202.0-blue)](LICENSE)
[![Contributions](https://img.shields.io/badge/contributions-welcome-blue)](https://github.com/paulcruse3/daplug-sql/issues)

`daplug-sql` wraps psycopg2 / mysql-connector with optimistic CRUD helpers and SNS event fan-out so your Postgres and MySQL services stay DRY and event-driven.

> 📎 **Agents** – a dedicated playbook lives in [`.agents/AGENTS.md`](.agents/AGENTS.md).

---

## ✨ Key Features

- **Single adapter factory** – `daplug_sql.adapter(**kwargs)` returns a ready-to-go adapter configured for Postgres or MySQL based on the `engine` parameter.
- **Optimistic CRUD** – Identifier-aware `insert`, `update`, `upsert`, and `delete` guard against duplicates and emit SNS events automatically.
- **Connection reuse** – Thread-safe cache reuses connections per endpoint/database/user/port/engine and lazily closes them.
- **Integration-tested** – `pipenv run integration` spins up both Postgres and MySQL via docker-compose and runs the real test suite.

---

## 🚀 Quick Start

### Installation

```bash
pip install daplug-sql
# pipenv install daplug-sql
# poetry add daplug-sql
# uv pip install daplug-sql
```

### Minimal Example

```python
from daplug_sql import adapter

sql = adapter(
    endpoint="127.0.0.1",
    database="daplug",
    user="svc",
    password="secret",
    engine="postgres",  # "mysql" also supported
)

sql.connect()
sql.insert(
    data={"customer_id": "abc123", "name": "Ada"},
    table="customers",
    identifier="customer_id",
)
record = sql.get("abc123", table="customers", identifier="customer_id")
print(record)
sql.close()
```

---

## ⚙️ Configuration

| Parameter            | Type    | Required | Description                                                                 |
|----------------------|---------|----------|-----------------------------------------------------------------------------|
| `endpoint`           | `str`   | ✅       | Host/IP of the Postgres/MySQL server.                                       |
| `database`           | `str`   | ✅       | Database/schema name.                                                       |
| `user`               | `str`   | ✅       | Database username.                                                          |
| `password`           | `str`   | ✅       | Database password.                                                          |
| `engine`             | `str`   | ➖       | `'postgres'` (default) or `'mysql'`.                                        |
| `autocommit`         | `bool`  | ➖       | Defaults to `True`; set `False` for manual transaction control.             |
| `sns_arn`            | `str`   | ➖       | SNS topic ARN used when publishing CRUD events.                              |
| `sns_endpoint`       | `str`   | ➖       | Optional SNS endpoint URL (e.g., LocalStack).                               |
| `sns_attributes`     | `dict`  | ➖       | Default SNS message attributes merged into every publish.                    |

### Per-Call Options

Every CRUD/query helper expects the target table and identifier column at call time so one adapter can manage multiple tables:

| Argument       | Description                                            |
|----------------|--------------------------------------------------------|
| `table`        | Table to operate on (`customers`, `orders`, etc.).      |
| `identifier`   | Column that uniquely identifies rows (`customer_id`).   |
| `commit`       | Override `autocommit` per call (`True`/`False`).        |
| `debug`        | Log SQL statements via the adapter logger when `True`.  |
| `sns_attributes` | Per-call attributes merged with defaults before publish. |
| `fifo_group_id` / `fifo_duplication_id` | Optional FIFO metadata passed straight to SNS. |
| `publish` | Set to `False` to skip the SNS publish for this call only (default `True`). |
| `publish_data` | Replace the published payload entirely (the row write is unchanged). |

### SNS Publishing

`SQLAdapter` inherits daplug-core's SNS publisher. Provide the topic details when constructing the adapter:

```python
sql = adapter(
    endpoint="127.0.0.1",
    database="daplug",
    user="svc",
    password="secret",
    engine="postgres",
    sns_arn="arn:aws:sns:us-east-1:123456789012:sql-events",
    sns_endpoint="http://localhost:4566",  # optional (LocalStack)
    sns_attributes={"service": "billing"},
)
```

- `sns_attributes` passed to `adapter(...)` become defaults for every publish.
- Each CRUD helper accepts its own `sns_attributes` to overlay call-specific metadata.
- FIFO topics are supported via the `fifo_group_id` and `fifo_duplication_id` kwargs on individual calls.

Example:

```python
sql.insert(
    data={"customer_id": "abc123", "name": "Ada"},
    table="customers",
    identifier="customer_id",
    sns_attributes={"event": "customer-created"},
    fifo_group_id="customers",
)
```

If `sns_arn` is omitted, publish calls are skipped automatically. To skip
a single call while keeping defaults intact, pass `publish=False`. To
publish a different payload than the row that was written, pass
`publish_data={...}`.

```python
sql.insert(data=row, table="customers", identifier="customer_id", publish=False)

sql.update(
    data=row,
    table="customers",
    identifier="customer_id",
    publish_data={"id": row["customer_id"], "event": "updated"},
)
```

---

## 🧭 Public API Cheat Sheet

| Method                     | Description                                                                                                   |
|----------------------------|---------------------------------------------------------------------------------------------------------------|
| `connect()`                                         | Opens a connection + cursor using the engine-specific connector.                                   |
| `close()`                                           | Closes the cursor/connection and evicts the cached connector.                                       |
| `commit(commit=True)`                               | Commits the underlying DB connection when `commit` is truthy.                                      |
| `insert(data, table, identifier, **kwargs)`         | Validates data, enforces uniqueness on the provided identifier, inserts the row, and publishes SNS. |
| `update(data, table, identifier, **kwargs)`         | Fetches the existing row, merges via `dict_merger`, runs `UPDATE`, publishes SNS.                   |
| `upsert(data, table, identifier, **kwargs)`         | Calls `update` when the row exists; falls back to `insert`.                                        |
| `get(identifier_value, table, identifier, **kwargs)`| Returns the first matching row or `None`.                                                         |
| `read(identifier_value, table, identifier, **kwargs)`| Alias of `get`.                                                                                   |
| `query(query, params, table, identifier, **kwargs)` | Executes a read-only statement (SELECT) and returns all rows as dictionaries.                       |
| `delete(identifier_value, table, identifier, **kwargs)` | Deletes the row, publishes SNS, and ignores missing rows.                                     |
| `create_index(table_name, index_columns)`           | Issues `CREATE INDEX index_col1_col2 ON table_name (col1, col2)` using safe identifiers.            |

> All identifier-based helpers sanitize names with `SAFE_IDENTIFIER` to prevent SQL injection through table/column inputs.

---

## 📚 Usage Examples

### Insert + Query (Postgres)

```python
sql = adapter(
    endpoint="127.0.0.1",
    database="daplug",
    user="svc",
    password="secret",
    engine="postgres",
)
sql.connect()

sql.insert(data={"sku": "W-1000", "name": "Widget", "cost": 99}, table="inventory", identifier="sku")
rows = sql.query(
    query="SELECT sku, name FROM inventory WHERE cost >= %(min_cost)s",
    params={"min_cost": 50},
    table="inventory",
    identifier="sku",
)
print(rows)
sql.close()
```

### Transactions (MySQL)

```python
sql = adapter(
    endpoint="127.0.0.1",
    database="daplug",
    user="svc",
    password="secret",
    engine="mysql",
    autocommit=False,
)
sql.connect()

try:
    sql.insert(data={"order_id": "O-1", "status": "pending"}, table="orders", identifier="order_id", commit=False)
    sql.update(data={"order_id": "O-1", "status": "shipped"}, table="orders", identifier="order_id", commit=False)
    sql.commit(True)
finally:
    sql.close()
```

### Per-call Table Overrides

```python
# Share one adapter across multiple tables by overriding table + identifier per call
sql.insert(data=payload, table="orders", identifier="order_id")
sql.create_index("orders", ["status", "created_at"])
```

---

## 🧪 Testing & Tooling

| Command                   | Description                                                                 |
|---------------------------|-----------------------------------------------------------------------------|
| `pipenv run lint`         | Runs pylint and exports HTML/JSON to `coverage/lint`.                       |
| `pipenv run type-check`   | Runs mypy using the new Protocol types.                                     |
| `pipenv run test`         | Executes the unit suite (mocks only).                                       |
| `pipenv run integration`  | Starts Postgres + MySQL via docker-compose and runs `tests/integration`.    |
| `pipenv run test_ci`      | Runs unit tests and integration tests sequentially (no Docker management). |
| `pipenv run coverage`     | Full coverage run producing HTML, XML, JUnit, and pretty reports.          |

Integration tests rely on `tests/integration/docker-compose.yml`. The CircleCI pipeline mirrors this by launching Postgres and MySQL sidecars, waiting for them to be reachable, and then executing `pipenv run coverage` so artifacts are published automatically.

---

## 🗂 Project Layout

```txt
daplug-sql/
├── daplug_sql/
│   ├── adapter.py           # SQLAdapter implementation
│   ├── exception.py         # Adapter-specific exceptions
│   ├── sql_connector.py     # Engine-aware connector wrapper
│   ├── sql_connection.py    # Connection caching decorators
│   ├── types/__init__.py    # Shared typing helpers (Protocols, aliases)
│   └── __init__.py          # Adapter factory export
├── tests/
│   ├── unit/                # Pure unit tests (mocks only)
│   └── integration/         # Integration tests (Postgres + MySQL)
├── tests/integration/docker-compose.yml
├── Pipfile / Pipfile.lock   # Runtime + dev dependencies
├── setup.py                 # Packaging metadata
├── README.md
└── .agents/AGENTS.md        # Automation/Triage playbook for agents
```

---

## 🤝 Contributing

1. Fork / branch (`git checkout -b feature/amazing`)
2. `pipenv install --dev`
3. Add/change code + tests
4. Run `pipenv run lint && pipenv run type-check && pipenv run test && pipenv run integration`
5. Open a pull request and tag `@dual`

---

## 📄 License

Apache License 2.0 – see [LICENSE](LICENSE).

---

Built to keep SQL integrations event-driven and zero-boilerplate.
