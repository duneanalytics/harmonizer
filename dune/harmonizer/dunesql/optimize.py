from sqlglot import exp
from sqlglot.expressions import replace_children
from sqlglot.optimizer import optimizer
from sqlglot.optimizer.annotate_types import TypeAnnotator


def optimize(expr, schema):
    """Optimize a DuneSQL expression (AST) according to a provided schema

    Involves fully qualifying all table and column names.
    Current rules supported are
    - casting types in equals"""
    annotators = TypeAnnotator.ANNOTATORS | {
        exp.HexString: lambda self, expr: self._annotate_with_type(expr, exp.DataType.Type.VARBINARY),
    }
    coerces_to = TypeAnnotator.COERCES_TO

    annotated_expr = optimizer.annotate_types(
        expression=optimizer.qualify(
            expression=expr,
            schema=schema,
        ),
        schema=schema,
        annotators=annotators,
        coerces_to=coerces_to,
    )
    return _cast_types_in_equals(annotated_expr, coerces_to=coerces_to)


def _handle_varchar_varbinary(e):
    # varchar column = hexstring: cast hexstring to varchar
    if isinstance(e.right, exp.HexString):
        return e.replace(
            type(e)(
                this=e.left,
                expression=exp.Cast(this=e.right, to=exp.DataType.build("varchar")),
            )
        )
    # varbinary column = string literal starting with 0x: unhex string literal
    if isinstance(e.right, exp.Literal) and e.right.is_string and e.right.this.startswith("0x"):
        return e.replace(
            type(e)(
                this=e.left,
                expression=exp.Unhex(this=e.right),
            )
        )
    # varbinary column = string literal
    if isinstance(e.right, exp.Literal):
        return e.replace(
            type(e)(
                this=e.left,
                expression=exp.Cast(this=e.right, to=exp.DataType.build("varbinary")),
            )
        )
    # hexstring = varchar column: cast hexstring to varchar
    if isinstance(e.left, exp.HexString):
        return e.replace(
            type(e)(
                this=exp.Cast(this=e.left, to=exp.DataType.build("varchar")),
                expression=e.right,
            )
        )
    # string literal starting with 0x = varbinary column: unhex string literal
    if isinstance(e.left, exp.Literal) and e.left.is_string and e.left.this.startswith("0x"):
        return e.replace(
            type(e)(
                this=exp.Unhex(this=e.left),
                expression=e.right,
            )
        )
    # string literal = varbinary column
    if isinstance(e.left, exp.Literal):
        return e.replace(
            type(e)(
                this=exp.Cast(this=e.left, to=exp.DataType.build("varbinary")),
                expression=e.right,
            )
        )
    if isinstance(e.left, exp.Column) and isinstance(e.right, exp.Column):
        # varchar column = varbinary column
        if e.left.type.this == exp.DataType.Type.VARCHAR:
            return e.replace(
                type(e)(
                    this=e.left,
                    expression=exp.Cast(this=e.right, to=exp.DataType.build("varchar")),
                )
            )
        # varbinary column = varchar column
        if e.right.type.this == exp.DataType.Type.VARCHAR:
            return e.replace(
                type(e)(
                    this=exp.Cast(this=e.left, to=exp.DataType.build("varchar")),
                    expression=e.right,
                )
            )
    raise ValueError("unreachable")


def _cast_types_in_equals(expression, coerces_to):
    """Ensure types in equals expressions are the same type, if possible"""
    replace_children(expression, _cast_types_in_equals, coerces_to=coerces_to)
    if isinstance(expression, (exp.EQ, exp.NEQ)):
        left_type, right_type = expression.left.type.this, expression.right.type.this

        # Treat comparisons between varbinary and varchar
        types = (left_type, right_type)
        if exp.DataType.Type.VARBINARY in types and exp.DataType.Type.VARCHAR in types:
            return _handle_varchar_varbinary(expression)

        # Otherwise use coercion hierarchy to cast one type to the other
        left_coerces_to, right_coerces_to = coerces_to.get(left_type, set()), coerces_to.get(right_type, set())
        # Cast left operand to type of right
        if right_type in left_coerces_to:
            cast = exp.Cast(this=expression.left, to=exp.DataType.build(right_type))
            return expression.replace(type(expression)(this=cast, expression=expression.right))
        # Cast right operand to type of left
        if left_type in right_coerces_to:
            cast = exp.Cast(this=expression.right, to=exp.DataType.build(left_type))
            return expression.replace(type(expression)(this=expression.left, expression=cast))
    return expression
