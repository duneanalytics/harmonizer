from sqlglot import exp
from sqlglot.expressions import replace_children
from sqlglot.optimizer import optimizer
from sqlglot.optimizer.annotate_types import TypeAnnotator


def optimize(expr, schema):
    """Optimize a DuneSQL expression (AST) according to a provided schema

    Involves fully qualifying all table and column names.
    Current rules supported are
    - casting types in equals"""
    annotators = TypeAnnotator.ANNOTATORS
    coerces_to = TypeAnnotator.COERCES_TO
    annotated_expr = optimizer.annotate_types(
        expression=optimizer.qualify_columns(
            expression=expr,
            schema=schema,
        ),
        schema=schema,
        annotators=annotators,
        coerces_to=coerces_to,
    )
    return _cast_types_in_equals(annotated_expr, coerces_to=coerces_to)


def _cast_types_in_equals(expression, coerces_to):
    """Ensure types in equals expressions are the same type, if possible"""
    replace_children(expression, _cast_types_in_equals, coerces_to=coerces_to)
    if isinstance(expression, exp.EQ):
        left_type, right_type = expression.left.type.this, expression.right.type.this
        left_coerces_to, right_coerces_to = coerces_to.get(left_type, set()), coerces_to.get(right_type, set())
        # downcast left to right type
        if right_type in left_coerces_to:
            cast = exp.Cast(this=expression.left, to=exp.DataType.build(right_type))
            return expression.replace(exp.EQ(this=cast, expression=expression.right))
        # downcast right to left type
        if left_type in right_coerces_to:
            cast = exp.Cast(this=expression.right, to=exp.DataType.build(left_type))
            return expression.replace(exp.EQ(this=expression.left, expression=cast))
    return expression
