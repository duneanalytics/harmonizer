from sqlglot.dialects.trino import Trino
from sqlglot.dialects.spark import Spark
from sqlglot.generator import Generator
from sqlglot.tokens import Tokenizer
from sqlglot import TokenType, transpile, exp


class DuneSQL(Trino):
    class Tokenizer(Trino.Tokenizer):
        HEX_STRINGS = ["0x", ("X'", "'")]

        KEYWORDS = Trino.Tokenizer.KEYWORDS | {"UINT256": TokenType.UBIGINT, "INT256": TokenType.BIGINT}

    class Generator(Trino.Generator):
        TRANSFORMS = Trino.Generator.TRANSFORMS | {
            exp.HexString: lambda self, e: hex(int(e.name)),
        }


sql = {
    "SELECT X'12A3'": "SELECT 0x12A3",
    "SELECT * FROM my_table WHERE col = 0xafcF2e06": "SELECT * FROM my_table WHERE col = 0xafcF2e06",
    "SELECT * FROM my_table WHERE col = X'deadbeef'": "SELECT * FROM my_table WHERE col = 0xdeadbeef",
}


def test_sql():
    for k, v in sql.items():
        print("---------")
        t = transpile(k, read=DuneSQL, write=DuneSQL)[0]
        print("got: ", t)
        print("exp: ", v)


if __name__ == "__main__":
    test_sql()
