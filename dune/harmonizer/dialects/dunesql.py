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
                    # explode_to_unnest,
                ]
            ),
        }

        TYPE_MAPPING = Trino.Generator.TYPE_MAPPING | {
            exp.DataType.Type.UBIGINT: "UINT256",
            exp.DataType.Type.BIGINT: "INT256",
        }
