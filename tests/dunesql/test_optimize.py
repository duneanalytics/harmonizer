import pytest
import sqlglot
from sqlglot.optimizer.qualify_columns import validate_qualify_columns

from dune.harmonizer.dunesql.dunesql import DuneSQL
from dune.harmonizer.dunesql.optimize import optimize

testcases = [
    {
        "schema": {"tbl": {"col": "double"}},
        "in": "SELECT col = 1 FROM tbl",
        "out": "SELECT tbl.col = CAST(1 AS DOUBLE) AS _col_0 FROM tbl",
    },
    {
        "schema": {"tbl": {"col": "varchar"}},
        "in": "SELECT col = 0xdeadbeef FROM tbl",
        "out": "SELECT tbl.col = CAST(0xdeadbeef AS VARCHAR) AS _col_0 FROM tbl",
    },
    {
        "schema": {"tbl": {"col": "varchar"}},
        "in": "SELECT col = '0xdeadbeef' FROM tbl",
        "out": "SELECT tbl.col = '0xdeadbeef' AS _col_0 FROM tbl",
    },
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT col = '0xdeadbeef' FROM tbl",
        "out": "SELECT tbl.col = FROM_HEX('0xdeadbeef') AS _col_0 FROM tbl",
    },
    {
        "schema": {"tbl": {"col": "varbinary"}},
        "in": "SELECT col = 0xdeadbeef FROM tbl",
        "out": "SELECT tbl.col = 0xdeadbeef AS _col_0 FROM tbl",
    },
]


def test_optimize_cast():
    for tc in testcases:
        dune_sql_expr = sqlglot.parse_one(tc["in"], read=DuneSQL)
        optimized = optimize(dune_sql_expr, schema=tc["schema"])
        validate_qualify_columns(optimized)
        print(tc)
        assert tc["out"] == optimized.sql(DuneSQL)


# case 1
# varbinary lit = varchar col
# 0xdead = col -> '0xdead' = col -> generated to 0xdead = col

# case 2 covered by generation
# varchar lit (hexstring) = varbinary col
# '0xdead' = col -> 0xdead = col

# case 3
# varchar lit (hexstring) = varchar col
# '0xdead' = col -> '0xdead' = col

# case 4
# varbinary lit = varbinary col
# 0xdead = col -> 0xdead = col
