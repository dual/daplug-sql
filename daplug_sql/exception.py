class SQLAdapterException(Exception):
    """Base exception for SQL adapter errors."""


class CreateTableException(SQLAdapterException):
    """Raised when create-table rules are violated."""
