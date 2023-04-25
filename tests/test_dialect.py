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


def test_force_string_with_0x_to_hexstring():
    assert "SELECT 0xdeadbeef" == sqlglot.transpile("SELECT '0xdeadbeef'", read="spark", write=DuneSQL)[0]
    assert (
        "SELECT 0xdeadbeef, LOWER(0xdeadbeef), 0x1 || 0x2"
        == sqlglot.transpile("SELECT '0xdeadbeef', lower('0xdeadbeef'), '0x1' || '0x2'", read="spark", write=DuneSQL)[0]
    )


def test_custom_types():
    sqlglot.parse_one("SELECT CAST(1 AS INT256)", read=DuneSQL)
    sqlglot.parse_one("SELECT CAST(1 AS UINT256)", read=DuneSQL)
    assert "SELECT CAST(1 AS UINT256)" == sqlglot.transpile("SELECT CAST(1 AS UINT256)", read=DuneSQL, write=DuneSQL)[0]


def test_explode_to_unnest():
    # plain select
    assert (
        sqlglot.transpile("SELECT explode(account_keys) FROM solana.transactions", read="postgres", write=DuneSQL)[0]
        == "SELECT col FROM solana.transactions CROSS JOIN UNNEST(account_keys) AS array_column(col)"
    )
    # alias
    assert (
        sqlglot.transpile(
            "SELECT explode(account_keys) AS exploded FROM solana.transactions", read="postgres", write=DuneSQL
        )[0]
        == "SELECT exploded FROM solana.transactions CROSS JOIN UNNEST(account_keys) AS array_column(exploded)"
    )
    # original select expression has no FROM clause, so should just be FROM UNNEST
    assert (
        sqlglot.transpile("SELECT explode(sequence(1, 2))", read="spark", write=DuneSQL)[0]
        == "SELECT col FROM UNNEST(SEQUENCE(1, 2)) AS array_column(col)"
    )
    # posexplode from a table
    assert sqlglot.transpile("SELECT posexplode(sequence(2, 3)) FROM solana.transactions", read="spark", write=DuneSQL)[
        0
    ] == " ".join(
        (
            "SELECT pos, col FROM solana.transactions",
            "CROSS JOIN UNNEST(SEQUENCE(2, 3)) WITH ORDINALITY AS array_column(col, pos)",
        )
    )
    # posexplode, no from
    assert (
        "SELECT pos, col FROM UNNEST(SEQUENCE(2, 3)) WITH ORDINALITY AS array_column(col, pos)"
        == sqlglot.transpile("SELECT posexplode(sequence(2, 3))", read="spark", write=DuneSQL)[0]
    )
    # explode with table alias
    assert (
        "SELECT col FROM table AS t CROSS JOIN UNNEST(t.c) AS array_column(col)"
        == sqlglot.transpile("SELECT explode(t.c) FROM table t", read="spark", write=DuneSQL)[0]
    )
    # posexplode with table alias
    assert (
        "SELECT pos, col FROM table AS t CROSS JOIN UNNEST(t.c) WITH ORDINALITY AS array_column(col, pos)"
        == sqlglot.transpile("SELECT posexplode(t.c) FROM table t", read="spark", write=DuneSQL)[0]
    )
