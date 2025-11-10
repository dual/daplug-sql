# 🔗 daplug-sql (da•plug)

> **Schema-Driven SQL Normalization & Event Publishing for Python**

[![CircleCI](https://circleci.com/gh/dual/daplug-sql.svg?style=shield)](https://circleci.com/gh/dual/daplug-sql)
[![Python](https://img.shields.io/badge/python-3.9%2B-blue)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-apache%202.0-blue)](LICENSE)
[![Contributions](https://img.shields.io/badge/contributions-welcome-blue)](https://github.com/dual/daplug-sql/issues)

`daplug-sql` gives you the same adapter ergonomics as `daplug-ddb`, but for relational databases. It wraps psycopg2 / mysql-connector with schema-aware CRUD helpers, optimistic updates, and SNS event fan-out so your Postgres and MySQL services stay DRY and event-driven.

> 📎 **Agents** – a dedicated playbook lives in [`.agents/AGENTS.md`](.agents/AGENTS.md).

---

## ✨ Key Features

- **Single adapter factory** – `daplug_sql.adapter(**kwargs)` returns a ready-to-go adapter configured for Postgres or MySQL based on the `engine` parameter.
- **Schema mapping** – Reuse OpenAPI / JSON schemas via daplug-core to validate payloads before touching the database.
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
    table="customers",
    identifier="customer_id",
    engine="postgres",  # "mysql" also supported
    schema_file="openapi.yml",
    model_schema="CustomerModel",
)

sql.connect()
sql.insert(data={"customer_id": "abc123", "name": "Ada"})
record = sql.get("abc123")
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
| `table`              | `str`   | ✅       | Default table name for CRUD helpers.                                        |
| `identifier`         | `str`   | ✅       | Column used to uniquely identify rows.                                      |
| `engine`             | `str`   | ➖       | `'postgres'` (default) or `'mysql'`.                                        |
| `autocommit`         | `bool`  | ➖       | Defaults to `True`; set `False` for manual transaction control.             |
| `model_schema`       | `str`   | ➖       | Key inside `model_schema_file` used for mapping writes.                     |
| `model_schema_file`  | `str`   | ➖       | Path to your OpenAPI/JSON schema file.                                      |
| `sns_*` kwargs       | Mixed   | ➖       | Standard daplug-core SNS configuration (ARN, endpoint, fifo options, etc.). |

---

## 🧭 Public API Cheat Sheet

| Method                     | Description                                                                                                   |
|----------------------------|---------------------------------------------------------------------------------------------------------------|
| `connect()`                | Opens a connection + cursor using the engine-specific connector.                                              |
| `close()`                  | Closes the cursor/connection and evicts the cached connector.                                                  |
| `commit(commit=True)`      | Commits the underlying DB connection when `commit` is truthy.                                                 |
| `create(**kwargs)`         | Alias of `insert`.                                                                                             |
| `insert(data, **kwargs)`   | Validates schema, enforces uniqueness on the identifier, inserts the row, and publishes SNS.                  |
| `update(data, **kwargs)`   | Fetches the existing row, merges the payload via `dict_merger`, runs `UPDATE`, publishes SNS.                  |
| `upsert(**kwargs)`         | Calls `update` when the row exists; falls back to `insert`.                                                   |
| `get(identifier_value, **kwargs)` | Returns the first matching row or `None`.                                                          |
| `read(identifier_value, **kwargs)`| Alias of `get` for parity with other daplug adapters.                                                   |
| `query(query, params, **kwargs)`  | Executes a read-only statement (SELECT) and returns all rows as dictionaries.                            |
| `delete(identifier_value, **kwargs)` | Deletes the row, publishes SNS, and ignores missing rows.                                        |
| `create_index(table_name, index_columns)` | Issues `CREATE INDEX index_col1_col2 ON table_name (col1, col2)` using safe identifiers. |

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
    table="inventory",
    identifier="sku",
    engine="postgres",
)
sql.connect()

sql.insert(data={"sku": "W-1000", "name": "Widget", "cost": 99})
rows = sql.query(
    query="SELECT sku, name FROM inventory WHERE cost >= %(min_cost)s",
    params={"min_cost": 50},
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
    table="orders",
    identifier="order_id",
    engine="mysql",
    autocommit=False,
)
sql.connect()

try:
    sql.insert(data={"order_id": "O-1", "status": "pending"}, commit=False)
    sql.update(data={"order_id": "O-1", "status": "shipped"}, commit=False)
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

Built to keep SQL integrations schema-aware, event-driven, and zero-boilerplate.
