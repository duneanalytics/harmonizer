from sqlglot import exp
from sqlglot.dialects.trino import Trino


class DuneSQL(Trino):
    class Tokenizer(Trino.Tokenizer):
        """Text -> Tokens"""

        HEX_STRINGS = ["0x", ("X’", "‘")]

    class Parser(Trino.Parser):
        """Tokens -> AST"""

        pass

    class Generator(Trino.Generator):
        """AST -> SQL"""

        TRANSFORMS = Trino.Generator.TRANSFORMS | {
            # Output hex strings as 0xdeadbeef
            exp.HexString: lambda self, e: hex(int(e.name)),
        }
