import sqlglot

from dune.harmonizer.dunesql import DuneSQL
from dune.harmonizer.dunesql.optimize import optimize


def test_with_schema():
    dune_sql_expr = sqlglot.parse_one("SELECT col = 1 FROM tbl", read=DuneSQL)
    optimized = optimize(dune_sql_expr, schema={"tbl": {"col": "double"}})
    assert "SELECT tbl.col = CAST(1 AS DOUBLE) AS _col_0 FROM tbl" == optimized.sql(DuneSQL)
