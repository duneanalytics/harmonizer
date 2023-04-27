import sqlglot

from dune.harmonizer.dialects.dunesql import DuneSQL, _looks_like_timestamp


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
        "SELECT 0xdeadbeef, 0xdeadbeef"
        == sqlglot.transpile("SELECT '0xdeadbeef', lower('0xdeadbeef')", read="spark", write=DuneSQL)[0]
    )
    assert (
        "SELECT * FROM table WHERE col = 0xdeadbeef"
        == sqlglot.transpile("SELECT * FROM table WHERE col = '0xdeadbeef'", read="spark", write=DuneSQL)[0]
    )


def test_remove_lower_around_hexstring():
    assert sqlglot.transpile("SELECT lower('0xdeadbeef')", read="spark", write=DuneSQL)[0] == "SELECT 0xdeadbeef"


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
    # explode with clashing column names
    assert (
        "SELECT col, col_2 FROM array_column CROSS JOIN UNNEST(SEQUENCE(2, 3)) AS array_column(col_2)"
        == sqlglot.transpile("SELECT col, explode(sequence(2, 3)) FROM array_column", read="spark", write=DuneSQL)[0]
    )
    # posexplode with clashing column names
    assert (
        " ".join(
            (
                "SELECT pos, col, pos_2, col_2 FROM array_column",
                "CROSS JOIN UNNEST(SEQUENCE(2, 3)) WITH ORDINALITY AS array_column(col_2, pos_2)",
            )
        )
        == sqlglot.transpile(
            "SELECT pos, col, posexplode(sequence(2, 3)) FROM array_column",
            read="spark",
            write=DuneSQL,
        )[0]
    )
    # posexplode with clashing name to table alias
    assert (
        " ".join(
            (
                "SELECT pos, col, pos_2, col_2 FROM tbl AS array_column",
                "CROSS JOIN UNNEST(SEQUENCE(2, 3)) WITH ORDINALITY AS array_column(col_2, pos_2)",
            )
        )
        == sqlglot.transpile(
            "SELECT pos, col, posexplode(sequence(2, 3)) FROM tbl AS array_column",
            read="spark",
            write=DuneSQL,
        )[0]
    )


def test_custom_function():
    assert (
        "SELECT BYTEARRAY_TO_BIGINT(col)"
        == sqlglot.transpile("SELECT bytea2numeric(col)", read="postgres", write=DuneSQL)[0]
    )


def test_cast_bool_strings():
    assert (
        "SELECT TRUE, FALSE, TRUE = TRUE, 'word'"
        == sqlglot.transpile("SELECT 'true', 'false', 'true' = true, 'word'", read="postgres", write=DuneSQL)[0]
    )
    # must be idempotent
    assert (
        "SELECT TRUE"
        == sqlglot.transpile(
            sqlglot.transpile("SELECT 'true'", read="postgres", write=DuneSQL)[0],
            read=DuneSQL,
        )[0]
    )


def test_looks_like_timestamp():
    assert _looks_like_timestamp("2023-01-01")
    assert _looks_like_timestamp("2023-01-01 00:00")
    assert _looks_like_timestamp("2023-01-01 00:00:00")
    assert not _looks_like_timestamp("2023-01-01 x")
    assert not _looks_like_timestamp("x 2023-01-01")


def test_cast_timestamp_strings():
    assert (
        "SELECT CAST('2023-01-01' AS TIMESTAMP)"
        == sqlglot.transpile("SELECT '2023-01-01'", read="postgres", write=DuneSQL)[0]
    )
    # must be idempotent
    assert (
        "SELECT CAST('2023-01-01' AS TIMESTAMP)"
        == sqlglot.transpile(
            sqlglot.transpile("SELECT '2023-01-01'", read="postgres", write=DuneSQL)[0],
            read=DuneSQL,
        )[0]
    )


def test_concat_of_0x_strings():
    assert (
        "SELECT BYTEARRAY_CONCAT(0xdeadbeef, 0x10)"
        == sqlglot.transpile("SELECT concat('0xdeadbeef', '0x10')", read="spark", write=DuneSQL)[0]
    )
    # doesn't handle other cases than exactly two arguments, but one argument is optimized away by SQLGlot
    assert (
        "SELECT 0x10, CONCAT(0x10, 0x20, 0x30)"
        == sqlglot.transpile("SELECT concat('0x10'), CONCAT('0x10', '0x20', '0x30')", read="spark", write=DuneSQL)[0]
    )
    # pipe operator
    assert (
        "SELECT BYTEARRAY_CONCAT(0xdeadbeef, 0x10)"
        == sqlglot.transpile("SELECT '0xdeadbeef' || '0x10'", read="spark", write=DuneSQL)[0]
    )
    # chained pipes
    assert (
        "SELECT BYTEARRAY_CONCAT(BYTEARRAY_CONCAT(0x10, 0x20), 0x30)"
        == sqlglot.transpile("SELECT '0x10' || '0x20' || '0x30'", read="spark", write=DuneSQL)[0]
    )
