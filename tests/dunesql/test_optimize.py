import pytest
import sqlglot

from dune.harmonizer.dunesql.dunesql import DuneSQL
from dune.harmonizer.dunesql.optimize import optimize


def test_optimize_cast_with_schema():
    dune_sql_expr = sqlglot.parse_one("SELECT col = 1 FROM tbl", read=DuneSQL)
    optimized = optimize(dune_sql_expr, schema={"tbl": {"col": "double"}})
    assert "SELECT tbl.col = CAST(1 AS DOUBLE) AS _col_0 FROM tbl" == optimized.sql(DuneSQL)


def test_optimize_fails():
    dune_sql_expr = sqlglot.parse_one("SELECT col = 1 FROM tbl", read=DuneSQL)
    with pytest.raises(sqlglot.errors.OptimizeError, match="Unknown column: col"):
        optimize(dune_sql_expr, schema={"tbl": {"x": "int"}})
