import pytest
import sqlglot
from sqlglot.optimizer.qualify_columns import validate_qualify_columns

from dune.harmonizer.dunesql.dunesql import DuneSQL
from dune.harmonizer.dunesql.optimize import optimize


def test_optimize_cast_with_schema():
    dune_sql_expr = sqlglot.parse_one("SELECT col = 1 FROM tbl", read=DuneSQL)
    optimized = optimize(dune_sql_expr, schema={"tbl": {"col": "double"}})
    validate_qualify_columns(optimized)
    assert "SELECT tbl.col = CAST(1 AS DOUBLE) AS _col_0 FROM tbl" == optimized.sql(DuneSQL)


def test_fail_with_qualified_columns():
    dune_sql_expr = sqlglot.parse_one("SELECT col FROM tbl", read=DuneSQL)
    optimized = optimize(dune_sql_expr, schema={"tbl": {"x": "int"}})
    with pytest.raises(sqlglot.errors.OptimizeError, match="Unknown table: '' for column 'col'"):
        validate_qualify_columns(optimized)
