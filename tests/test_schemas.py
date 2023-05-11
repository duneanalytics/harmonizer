import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from dune.harmonizer.schemas import schema_from_sqlite


@pytest.fixture
def schema_db():
    path = Path(__file__).parent / "tmp.db"
    table_name = "schemas"
    conn = sqlite3.connect(str(path))
    with closing(conn.cursor()) as cursor:
        cursor.execute(f"CREATE TABLE {table_name} (table_name, column_name, sqlglot_type)")
        cursor.executemany(
            f"INSERT INTO {table_name} VALUES(?, ?, ?)",
            [("tbl", "col", "int"), ("tbl", "x", "boolean"), ("other_tbl", "z", "text")],
        )
        conn.commit()
    yield path, table_name
    path.unlink()  # delete database


def test_schema_from_sqlite(schema_db):
    db, name = schema_db
    schema = schema_from_sqlite(db, name)
    assert schema["tbl"]["col"] == "int"
