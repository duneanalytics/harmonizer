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
