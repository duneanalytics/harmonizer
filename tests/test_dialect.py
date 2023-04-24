import sqlglot

from dune.harmonizer.dialects.dunesql import DuneSQL


def test_parse_hexstring():
    assert sqlglot.parse_one("SELECT X'deadbeef'", read="trino") == sqlglot.parse_one("SELECT 0xdeadbeef", read=DuneSQL)
    assert sqlglot.parse_one("SELECT x'deadbeef'", read="postgres") == sqlglot.parse_one(
        "SELECT 0xdeadbeef", read=DuneSQL
    )


def test_generate_hexstring():
    assert "SELECT 0xdeadbeef" == sqlglot.transpile("SELECT X'deadbeef'", read="trino", write=DuneSQL)[0]
    assert "SELECT 0xdeadbeef" == sqlglot.transpile("SELECT x'deadbeef'", read="postgres", write=DuneSQL)[0]
    assert "SELECT 0xdeadbeef" == sqlglot.transpile("SELECT X'deadbeef'", read=DuneSQL, write=DuneSQL)[0]
    assert "SELECT 0xdeadbeef" == sqlglot.transpile("SELECT 0xdeadbeef", read=DuneSQL, write=DuneSQL)[0]


def test_custom_types():
    sqlglot.parse_one("SELECT CAST(1 AS INT256)", read=DuneSQL)
    sqlglot.parse_one("SELECT CAST(1 AS UINT256)", read=DuneSQL)
    assert "SELECT CAST(1 AS UINT256)" == sqlglot.transpile("SELECT CAST(1 AS UINT256)", read=DuneSQL, write=DuneSQL)[0]


def test_explode_to_unnest():
    # plain select
    assert (
        "SELECT col FROM solana.transactions CROSS JOIN UNNEST(account_keys) AS array_column(col)"
        == sqlglot.transpile("SELECT explode(account_keys) FROM solana.transactions", read="postgres", write=DuneSQL)[0]
    )
    # alias
    assert (
        "SELECT exploded FROM solana.transactions CROSS JOIN UNNEST(account_keys) AS array_column(exploded)"
        == sqlglot.transpile(
            "SELECT explode(account_keys) AS exploded FROM solana.transactions", read="postgres", write=DuneSQL
        )[0]
    )
    # original select expression has no FROM clause
    assert (
        "SELECT col FROM  CROSS JOIN UNNEST(SEQUENCE(1, 2)) AS array_column(col)"
        == sqlglot.transpile("SELECT explode(sequence(1, 2))", read="spark", write=DuneSQL)[0]
    )
