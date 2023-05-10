import re

from sqlglot import TokenType, exp, transforms
from sqlglot.dialects.trino import Trino


def replace_0x_strings_with_hex_strings(expression: exp.Expression):
    """Recursively replace string literals starting with '0x' with the equivalent HexString"""
    return expression.transform(
        lambda e: exp.HexString(this=e.args["this"][2:])
        if isinstance(e, exp.Literal) and e.args["is_string"] and e.args["this"].startswith("0x")
        else e
    )


def remove_lower_around_hex_strings(expression: exp.Expression):
    """Remove the LOWER() function around hex strings"""
    return expression.transform(
        lambda e: e.args["this"] if isinstance(e, exp.Lower) and isinstance(e.args["this"], exp.HexString) else e
    )


def rename_bytea2numeric_to_bytearray_to_bigint(expression: exp.Expression):
    """Rename our custom UDF `bytea2numeric` to our Trino function `bytearray_to_bigint`"""
    return expression.transform(
        lambda e: exp.Anonymous(this="bytearray_to_bigint", expressions=e.expressions)
        if isinstance(e, exp.Anonymous) and e.name.lower() == "bytea2numeric"
        else e
    )


def cast_boolean_strings(expression: exp.Expression):
    """Explicitly cast strings with booleans in them to booleans

    Spark and Postgres implicitly convert strings with 'true' or 'false' into booleans when needed"""
    return expression.transform(
        lambda e: exp.Boolean(this=True if e.this.lower() == "true" else False)
        if isinstance(e, exp.Literal)
        and e.args["is_string"]
        and (e.this.lower() == "true" or e.this.lower() == "false")
        and (not isinstance(e.parent, exp.Cast))
        else e
    )


date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
timestamp_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$")
timestamp_regex_seconds = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def _looks_like_timestamp(e: str):
    return timestamp_regex.match(e) or timestamp_regex_seconds.match(e) or date_regex.match(e)


def cast_date_strings(expression: exp.Expression):
    """Explicitly cast all strings that look like timestamps to timestamps

    Spark and Postgres implicitly convert strings like this into timestamps when needed"""
    return expression.transform(
        lambda e: exp.Cast(this=e, to=exp.DataType.build("timestamp"))
        if isinstance(e, exp.Literal)
        and e.args["is_string"]
        and _looks_like_timestamp(e.this)
        and (not isinstance(e.parent, exp.Cast))
        else e
    )


def concat_of_hex_string_to_bytearray_concat(expression: exp.Expression):
    """Replace any CONCAT call with bytearray_concat function call if arguments are hex strings"""
    return expression.transform(
        lambda e: exp.Anonymous(this="bytearray_concat", expressions=e.expressions)
        if isinstance(e, exp.Concat)
        and all(isinstance(arg, exp.HexString) for arg in e.expressions)
        and len(e.expressions) == 2  # bytearray_concat isn't variadic; only supports 2 arguments
        else e
    )


def pipe_expression_to_bytearray_concat_call(e: exp.Expression):
    """Replace the pipe operator || in this expression with a bytearray_concat function call

    If arguments are hex strings. Not recursive!
    """
    if isinstance(e, exp.DPipe) and isinstance(e.right, exp.HexString):
        if isinstance(e.left, exp.HexString):
            return exp.Anonymous(this="bytearray_concat", expressions=[e.left, e.right])
        elif isinstance(e.left, exp.DPipe):
            # call recursively on left to handle nested pipes
            return exp.Anonymous(
                this="bytearray_concat", expressions=[pipe_of_hex_strings_to_bytearray_concat(e.left), e.right]
            )
    return e


def pipe_of_hex_strings_to_bytearray_concat(expression: exp.Expression):
    """Replace all || with bytearray_concat function call if arguments are hex strings"""
    return expression.transform(pipe_expression_to_bytearray_concat_call)


class DuneSQL(Trino):
    """The DuneSQL dialect is the dialect used to execute SQL queries on Dune's crypto data sets

    DuneSQL is the Trino dialect with slight modifications."""

    class Tokenizer(Trino.Tokenizer):
        """Text -> Tokens"""

        HEX_STRINGS = ["0x", ("X'", "'")]
        KEYWORDS = Trino.Tokenizer.KEYWORDS | {
            "UINT256": TokenType.UBIGINT,
            "INT256": TokenType.BIGINT,
        }

    class Parser(Trino.Parser):
        """Tokens -> AST"""

        TYPE_TOKENS = Trino.Parser.TYPE_TOKENS | {TokenType.UBIGINT, TokenType.BIGINT}

    class Generator(Trino.Generator):
        """AST -> SQL"""

        TYPE_MAPPING = Trino.Generator.TYPE_MAPPING | {
            exp.DataType.Type.UBIGINT: "UINT256",
            exp.DataType.Type.BIGINT: "INT256",
        }
        TRANSFORMS = Trino.Generator.TRANSFORMS | {
            exp.HexString: lambda self, e: f"0x{e.this}",
            exp.Select: transforms.preprocess(
                [
                    transforms.eliminate_qualify,
                    transforms.explode_to_unnest,
                    replace_0x_strings_with_hex_strings,
                    remove_lower_around_hex_strings,
                    rename_bytea2numeric_to_bytearray_to_bigint,
                    cast_boolean_strings,
                    cast_date_strings,
                    concat_of_hex_string_to_bytearray_concat,
                    pipe_of_hex_strings_to_bytearray_concat,
                ]
            ),
        }
