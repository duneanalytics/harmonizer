import pytest
import sqlglot
from sqlglot.optimizer.qualify_columns import validate_qualify_columns

from dune.harmonizer.dunesql.dunesql import DuneSQL
from dune.harmonizer.dunesql.optimize import optimize

testcases = [
    # x = y
    {
        "schema": {"tbl": {"col": "double"}},
        "in": "SELECT col = 1 FROM tbl",
        "out": """SELECT "tbl"."col" = CAST(1 AS DOUBLE) AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "double"}},
        "in": "SELECT 1 = col FROM tbl",
        "out": """SELECT CAST(1 AS DOUBLE) = "tbl"."col" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # x = y
    {
        "schema": {"tbl": {"col": "varchar"}},
        "in": "SELECT col = 0xdeadbeef FROM tbl",
        "out": """SELECT "tbl"."col" = CAST(0xdeadbeef AS VARCHAR) AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "varchar"}},
        "in": "SELECT 0xdeadbeef = col FROM tbl",
        "out": """SELECT CAST(0xdeadbeef AS VARCHAR) = "tbl"."col" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # x = y
    {
        "schema": {"tbl": {"col": "varchar"}},
        "in": "SELECT col = '0xdeadbeef' FROM tbl",
        "out": """SELECT "tbl"."col" = '0xdeadbeef' AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "varchar"}},
        "in": "SELECT '0xdeadbeef' = col FROM tbl",
        "out": """SELECT '0xdeadbeef' = "tbl"."col" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # x = y
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT col = '0xdeadbeef' FROM tbl",
        "out": """SELECT "tbl"."col" = FROM_HEX('0xdeadbeef') AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT '0xdeadbeef' = col FROM tbl",
        "out": """SELECT FROM_HEX('0xdeadbeef') = "tbl"."col" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # # x = y
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT col = 'a string' FROM tbl",
        "out": """SELECT "tbl"."col" = CAST('a string' AS VARBINARY) AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT 'a string' = col FROM tbl",
        "out": """SELECT CAST('a string' AS VARBINARY) = "tbl"."col" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # x = y
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT col = 0xdeadbeef FROM tbl",
        "out": """SELECT "tbl"."col" = 0xdeadbeef AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT 0xdeadbeef = col FROM tbl",
        "out": """SELECT 0xdeadbeef = "tbl"."col" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # x = y
    {
        "schema": {"tbl": {"col": "varbinary", "col2": "varchar"}},
        "in": "SELECT col = col2 FROM tbl",
        "out": """SELECT CAST("tbl"."col" AS VARCHAR) = "tbl"."col2" AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
    # y = x
    {
        "schema": {"tbl": {"col": "varbinary", "col2": "varchar"}},
        "in": "SELECT col2 = col FROM tbl",
        "out": """SELECT "tbl"."col2" = CAST("tbl"."col" AS VARCHAR) AS "_col_0" FROM "tbl" AS "tbl""" + '"',
    },
]


@pytest.mark.parametrize("tc", testcases)
def test_optimize_cast(tc):
    dune_sql_expr = sqlglot.parse_one(tc["in"], read=DuneSQL)
    optimized = optimize(dune_sql_expr, schema=tc["schema"])
    validate_qualify_columns(optimized)
    assert tc["out"] == optimized.sql(DuneSQL)
