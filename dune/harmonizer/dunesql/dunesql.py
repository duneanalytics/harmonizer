from sqlglot import TokenType, exp, transforms
from sqlglot.dialects.trino import Trino

from dune.harmonizer.dunesql.transform import (
    cast_boolean_strings,
    cast_date_strings,
    concat_of_hex_string_to_bytearray_concat,
    pipe_of_hex_strings_to_bytearray_concat,
    remove_calls_on_hex_strings,
    rename_bytea2numeric_to_bytearray_to_bigint,
    replace_0x_strings_with_hex_strings,
)


class DuneSQL(Trino):
    """The DuneSQL dialect is the dialect used to execute SQL queries on Dune's crypto data sets

    DuneSQL is the Trino dialect with slight modifications."""

    class Tokenizer(Trino.Tokenizer):
        """Text -> Tokens"""

        HEX_STRINGS = ["0x", ("X'", "'")]
        KEYWORDS = Trino.Tokenizer.KEYWORDS | {
            "UINT256": TokenType.UINT256,
            "INT256": TokenType.INT256,
        }

    class Parser(Trino.Parser):
        """Tokens -> AST"""

        pass

    class Generator(Trino.Generator):
        """AST -> SQL"""

        TYPE_MAPPING = Trino.Generator.TYPE_MAPPING | {
            exp.DataType.Type.UINT256: "UINT256",
            exp.DataType.Type.INT256: "INT256",
        }
        TRANSFORMS = Trino.Generator.TRANSFORMS | {
            exp.HexString: lambda self, e: f"0x{e.this}",
            exp.Select: transforms.preprocess(
                [
                    # Transforms from SQLGlot
                    transforms.eliminate_qualify,
                    transforms.explode_to_unnest,
                    # Custom transforms
                    rename_bytea2numeric_to_bytearray_to_bigint,
                    cast_boolean_strings,
                    cast_date_strings,
                    # Optimizations
                    replace_0x_strings_with_hex_strings,  # should happen at parse time?
                    remove_calls_on_hex_strings,
                    concat_of_hex_string_to_bytearray_concat,
                    pipe_of_hex_strings_to_bytearray_concat,
                ]
            ),
        }
