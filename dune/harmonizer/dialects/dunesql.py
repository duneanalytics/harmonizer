from sqlglot.dialects.trino import Trino
from sqlglot import exp

class DuneSQL(Trino):
    class Tokenizer(Trino.Tokenizer):
        HEX_STRINGS = ["0x", ("X'", "'")]

    class Generator(Trino.Generator):
        TRANSFORMS = Trino.Generator.TRANSFORMS | {
            # this transform will ensure that hex strings are always
            # output as 0xdeadbeef, and not as X'deadbeef'
            exp.HexString: lambda self, e: hex(int(e.name)),
        }
