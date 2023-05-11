import sqlite3
from collections import defaultdict
from contextlib import closing
from itertools import groupby


def schema_from_sqlite(path, schema_table_name):
    """Load a SQLGlot compatible dictionary of table schemas from a SQLite database.

    Assumes path is a SQLite database with a table `table_name`, with columns `table_name`, `column_name`
    and `sqlglot_type`, where `sqlglot_type` is one of the SQLGlot DataType.Type enum values.
    """
    with closing(sqlite3.connect(path).cursor()) as cursor:
        rows = cursor.execute(
            f"select table_name, column_name, sqlglot_type from {schema_table_name} order by table_name, column_name"
        ).fetchall()

    trino_schema = defaultdict(dict)
    for schema_table_name, columns in groupby(rows, key=lambda t: t[0]):  # group by table name
        for _, name, type_ in columns:
            trino_schema[schema_table_name][name] = type_
    return trino_schema
