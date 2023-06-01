import re

from sqlglot import exp


def replace_0x_strings_with_hex_strings(expression: exp.Expression):
    """Recursively replace string literals starting with '0x' with the equivalent HexString"""
    return expression.transform(
        lambda e: exp.HexString(this=e.this[2:])
        if isinstance(e, exp.Literal) and e.is_string and e.this.startswith("0x")
        # workaround for optimization: don't force hex string in binary expressions if we have type information
        and not (
            (
                isinstance(e.parent, (exp.EQ, exp.NEQ))
                and e.parent.left.type is not None
                and e.parent.right.type is not None
            )
            or isinstance(e.parent, exp.Unhex)
            or (isinstance(e.parent, exp.Cast) and e.parent.this.type is not None)
        )
        else e
    )


def remove_calls_on_hex_strings(expression: exp.Expression):
    """Remove LOWER(), FROM_HEX(), and (TRY)CAST functions used on hex strings, since hex strings are varbinary"""
    return expression.transform(
        lambda e: e.this
        if isinstance(e.this, exp.HexString)
        and (
            isinstance(e, (exp.Lower, exp.Unhex))
            or (isinstance(e, (exp.Cast, exp.TryCast)) and e.to.this == exp.DataType.Type.VARBINARY)
        )
        else e
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
        and e.is_string
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
        and e.is_string
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


def remove_varchar_lengths_in_casts(expression: exp.Expression):
    """Remove lengths from varchar casts as they would otherwise truncate in dunesql"""
    return expression.transform(
        lambda e: None
        if isinstance(e, exp.DataTypeSize)
        and isinstance(e.parent, exp.DataType) and e.parent.is_type(exp.DataType.Type.VARCHAR)
        else e
    )
