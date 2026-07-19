"""Backward-compatible database entry point."""

from .storage.sqlite import SQLiteDatabase

Database = SQLiteDatabase

__all__ = ["Database"]
