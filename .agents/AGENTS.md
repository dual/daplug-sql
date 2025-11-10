# 🤖 Agent Tutorial – Operating daplug-sql

This guide shows field agents how to integrate applications with `daplug-sql`. Follow the sections in order to connect, run operations, and interpret results.

---

## 1. Instantiate an Adapter

Use the factory function from `daplug_sql` and pass the connection details. Choose the engine (`postgres` or `mysql`) and supply SNS info if you need event publishing. Table/identifier names are supplied per method call.

```python
from daplug_sql import adapter

sql = adapter(
    endpoint="127.0.0.1",
    database="daplug",
    user="svc",
    password="secret",
    engine="postgres",  # or "mysql"
)
```

| Argument         | Purpose                                         |
|------------------|-------------------------------------------------|
| `endpoint`       | Host/IP of the target DB                        |
| `database`       | Database/schema name                            |
| `user`/`password`| Authentication credentials                      |
| `engine`         | `'postgres'` (default) or `'mysql'`             |
| `sns_arn`        | SNS topic ARN for CRUD event fan-out            |
| `sns_endpoint`   | Optional SNS endpoint URL (LocalStack)          |
| `sns_attributes` | Default SNS attributes merged into every event  |

Always pass `table` and `identifier` to each CRUD/query call so a single adapter can target any table without reopening connections.

---

## 2. Connect & Close

`connect()` opens the DB connection and cursor, and `close()` tears them down (and evicts the cached connector).

```python
sql.connect()
# ... run operations ...
sql.close()
```

> Agents should call `close()` in `finally` blocks to release cached connections.

---

## 3. Insert & Publish Events

`insert` validates the payload, checks the identifier for duplicates, inserts the row, and publishes SNS events when configured.

```python
payload = {"customer_id": "abc123", "name": "Ada"}
sql.insert(data=payload, table="customers", identifier="customer_id")
```

You can override `table` or `identifier` per call:

```python
sql.insert(data=payload, table="orders", identifier="order_id")
```

---

## 4. Retrieve Data

- `get(identifier_value)` returns a single row (dict or `None`).
- `read(identifier_value)` is an alias for parity with other daplug adapters.
- `query(query=..., params=...)` executes a read-only SQL statement and returns a list of rows.

```python
row = sql.get("abc123", table="customers", identifier="customer_id")
rows = sql.query(
    query="SELECT customer_id, name FROM customers WHERE status = %(status)s",
    params={"status": "active"},
    table="customers",
    identifier="customer_id",
)
```

> Only SELECT-style queries are allowed in `query`; destructive statements raise `READ_ONLY` errors.

---

## 5. Update vs Upsert

- `update` merges the existing row with the payload using `dict_merger`. It raises `NOT_EXISTS` when the identifier is missing.
- `upsert` calls `update` if the row exists; otherwise it falls back to `insert`.

```python
sql.update(data={"customer_id": "abc123", "status": "vip"}, table="customers", identifier="customer_id")
sql.upsert(data={"customer_id": "new", "status": "trial"}, table="customers", identifier="customer_id")
```

Both methods publish SNS events with the updated payload.

---

## 6. Delete Rows

`delete(identifier_value)` removes the row and publishes SNS with the identifier payload. Missing rows are ignored.

```python
sql.delete("abc123", table="customers", identifier="customer_id")
```

---

## 7. Create Indexes Safely

`create_index(table_name, index_columns)` builds an index using sanitized identifiers.

```python
sql.create_index("customers", ["status", "created_at"])
```

If an invalid identifier is supplied (e.g., contains spaces or starts with a digit), the adapter raises `ValueError` before executing SQL.

---

## 8. Transactions (optional)

Adapters default to `autocommit=True`. To perform manual transactions, set `autocommit=False`, pass `commit=False` to each write, and call `commit(True)` when ready.

```python
sql = adapter(..., autocommit=False)
sql.connect()
try:
    sql.insert(data=payload, table="customers", identifier="customer_id", commit=False)
    sql.update(data=payload, table="customers", identifier="customer_id", commit=False)
    sql.commit(True)
finally:
    sql.close()

### SNS Events

- Pass `sns_arn` (and optionally `sns_endpoint`, `sns_attributes`) when constructing the adapter.
- Per call, supply `sns_attributes`, `fifo_group_id`, or `fifo_duplication_id` to annotate messages.

```python
sql = adapter(..., sns_arn="arn:aws:sns:us-east-1:123456789012:sql-events")
sql.insert(
    data=payload,
    table="customers",
    identifier="customer_id",
    sns_attributes={"event": "customer-created"},
    fifo_group_id="customers",
)
```
```

---

## 9. Environment Overrides (CI / Containers)

Integration tests and CI jobs read host/port credentials from:

| Engine   | Environment variables                                    |
|----------|----------------------------------------------------------|
| Postgres | `SQL_POSTGRES_HOST`, `SQL_POSTGRES_PORT`, `SQL_POSTGRES_USER`, `SQL_POSTGRES_PASSWORD`, `SQL_POSTGRES_DB` |
| MySQL    | `SQL_MYSQL_HOST`, `SQL_MYSQL_PORT`, `SQL_MYSQL_USER`, `SQL_MYSQL_PASSWORD`, `SQL_MYSQL_DB`               |

Agents spinning up external databases should export these variables to match their environment.

---

## 10. Troubleshooting Checklist

1. **Connection errors** – ensure DB ports are reachable and env vars point to the right host/port.
2. **Identifier errors** – confirm table/column names match `[A-Za-z_][A-Za-z0-9_]*`.
3. **SNS not firing** – verify `sns_arn`, `sns_endpoint`, or other daplug-core settings passed to `adapter`.
4. **Payload shape issues** – ensure each payload includes the identifier column required by the table you pass to the method.

---

Keep this tutorial handy whenever you need to operate the SQL adapter from automations or scripts.
