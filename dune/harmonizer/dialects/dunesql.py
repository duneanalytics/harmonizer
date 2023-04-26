import re

from sqlglot import TokenType, exp, transforms
from sqlglot.dialects.trino import Trino


def explode_to_unnest(expression: exp.Expression):
    """Convert posexplode, explode, and aliased explode to unnest"""
    if isinstance(expression, exp.Select):
        for e in expression.args.get("expressions", []):
            if isinstance(e, exp.Posexplode):
                explode_expression = e.args["this"]

                # Remove the `posexplode()` expression from the select
                expression.args["expressions"].remove(e)

                # If the SELECT has a FROM, do a CROSS JOIN with the UNNEST,
                # otherwise, just do SELECT ... FROM UNNEST
                unnest = exp.Unnest(expressions=[explode_expression], alias="array_column(col, pos)", ordinality=True)
                if expression.args.get("from") is not None:
                    join = exp.Join(this=unnest, kind="CROSS")
                    expression = expression.select("pos", "col").join(join)
                else:
                    expression = expression.select("pos", "col").from_(unnest)
                continue

            elif isinstance(e, exp.Explode):
                unnested_column_name = "col"
                explode_expression = e.args["this"]

            elif isinstance(e, exp.Alias):
                if not isinstance(e.args["this"], exp.Explode):
                    continue
                unnested_column_name = e.alias
                explode_expression = e.args["this"].args["this"]

            # This is not a (pos)explode expression
            else:
                continue

            # Remove the `explode()` expression from the select
            expression.args["expressions"].remove(e)

            # If the SELECT has a FROM, do a CROSS JOIN with the UNNEST,
            # otherwise, just do SELECT ... FROM UNNEST
            unnest = exp.Unnest(expressions=[explode_expression], alias=f"array_column({unnested_column_name})")
            if expression.args.get("from") is not None:
                join = exp.Join(this=unnest, kind="CROSS")
                expression = expression.select(unnested_column_name).join(join)
            else:
                expression = expression.select(unnested_column_name).from_(unnest)
    return expression


def replace_0x_strings_with_hex_strings(expression: exp.Expression):
    """Recursively replace string literals starting with '0x' with the equivalent HexString"""
    return expression.transform(
        lambda e: exp.HexString(this=int(e.this, 16))
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
        else e
    )


date_regex = re.compile(r"^\d{4}-\d{2}-\d{2}$")
timestamp_regex = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$")
timestamp_regex_seconds = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$")


def _looks_like_timestamp(e: str):
    return timestamp_regex.match(e) or timestamp_regex_seconds.match(e) or date_regex.match(e)


def cast_date_strings(expression: exp.Expression):
    """Explicitly cast strings that look like timestamps to timestamps

    Spark and Postgres implicitly convert strings like this into timestamps when needed"""
    return expression.transform(
        lambda e: exp.Cast(this=e, to=exp.DataType.build("timestamp"))
        if isinstance(e, exp.Literal) and e.args["is_string"] and _looks_like_timestamp(e.this)
        else e
    )


def concat_of_hex_string_to_bytearray_concat(expression: exp.Expression):
    """Replace CONCAT with bytearray_concat function call if arguments are hex strings"""
    return expression.transform(
        lambda e: exp.Anonymous(this="bytearray_concat", expressions=e.expressions)
        if isinstance(e, exp.Concat)
        and all(isinstance(arg, exp.HexString) for arg in e.expressions)
        and len(e.expressions) == 2  # bytearray_concat isn't variadic; only supports 2 arguments
        else e
    )


def pipe_of_hex_strings(expression: exp.Expression):
    """Replace || with bytearray_concat function call if arguments are hex strings"""
    return expression.transform(
        lambda e: exp.Anonymous(this="bytearray_concat", expressions=[e.left, e.right])
        if isinstance(e, exp.DPipe) and isinstance(e.left, exp.HexString) and isinstance(e.right, exp.HexString)
        else e
    )


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

        TRANSFORMS = Trino.Generator.TRANSFORMS | {
            # Output hex strings as 0xdeadbeef
            exp.HexString: lambda self, e: hex(int(e.this)),
            exp.Select: transforms.preprocess(
                [
                    explode_to_unnest,
                    replace_0x_strings_with_hex_strings,
                    remove_lower_around_hex_strings,
                    rename_bytea2numeric_to_bytearray_to_bigint,
                    cast_boolean_strings,
                    cast_date_strings,
                    concat_of_hex_string_to_bytearray_concat,
                    pipe_of_hex_strings,
                ]
            ),
        }

        TYPE_MAPPING = Trino.Generator.TYPE_MAPPING | {
            exp.DataType.Type.UBIGINT: "UINT256",
            exp.DataType.Type.BIGINT: "INT256",
        }
