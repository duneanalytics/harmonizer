from sqlglot import TokenType, exp, transforms
from sqlglot.dialects.trino import Trino


def explode_to_unnest(expression: exp.Expression):
    """Convert explode to cross join unnest"""
    if isinstance(expression, exp.Select):
        for e in expression.args.get("expressions", []):
            # Handle either an aliased explode select, or a plain explode select
            explode_alias = None
            if isinstance(e, exp.Alias):
                if isinstance(e.args["this"], exp.Explode):
                    explode_alias = e.alias
                    explode = e.args["this"]
                    to_remove = e
                    explode_expression = explode.args["this"]
                else:
                    continue
            elif isinstance(e, exp.Explode):
                explode = e
                to_remove = e
                explode_expression = explode.args["this"]
            else:
                continue

            array_column_name = "array_column"
            unnested_column_name = explode_alias or "col"
            unnest = exp.Unnest(expressions=[explode_expression], alias=f"{array_column_name}({unnested_column_name})")
            join = exp.Join(this=unnest, kind="CROSS")
            # Remove the `explode()` expression from the select
            expression.args["expressions"].remove(to_remove)
            # Add an empty FROM expression so that there's always a FROM even if we don't select from a table,
            # e.g. if the original expression is `select explode(sequence(...)))`
            expression = expression.select(unnested_column_name).from_(expressions=[]).join(join)
    return expression


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
            exp.HexString: lambda self, e: hex(int(e.name)),
            exp.Select: transforms.preprocess([explode_to_unnest]),
        }

        TYPE_MAPPING = Trino.Generator.TYPE_MAPPING | {
            exp.DataType.Type.UBIGINT: "UINT256",
            exp.DataType.Type.BIGINT: "INT256",
        }
