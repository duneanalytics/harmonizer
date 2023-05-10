import re

import sqlglot
from sqlglot import ParseError
from sqlglot.optimizer.annotate_types import annotate_types

from dune.harmonizer.custom_transforms import (
    add_warnings_and_banner,
    double_quoted_param_left_placeholder,
    double_quoted_param_right_placeholder,
    fix_bytearray_param,
    parameter_placeholder,
    postgres_transforms,
    spark_transforms,
    v1_tables_to_v2_tables,
)
from dune.harmonizer.dialects.dunesql import DuneSQL
from dune.harmonizer.errors import DuneTranslationError


def _clean_dataset(dataset):
    if dataset is None:
        return None
    for d in ("gnosis", "optimism", "bnb", "polygon", "ethereum"):
        if d in dataset.lower():
            return d
    raise ValueError(f"Unknown dataset: {dataset}")


def _translate_query(query, sqlglot_dialect, dataset=None, syntax_only=False):
    """Translate a query using SQLGLot plus custom rules"""
    try:
        # Insert placeholders for the parameters we use in Dune (`{{ param }}`), SQLGlot doesn't handle those
        parameters = re.findall("({{.*?}})", query, flags=re.IGNORECASE)
        parameter_map = {parameter_placeholder(p): p for p in parameters}
        for replace, original in parameter_map.items():
            query = query.replace(original, replace)

        original_query = query
        if sqlglot_dialect == "postgres":
            # Update bytearray syntax
            original_query = original_query.replace("\\x", "0x")

        # Transpile to Trino
        # query = sqlglot.transpile(query, read=sqlglot_dialect, write="trino")[0]
        query_tree = sqlglot.parse_one(query, read=sqlglot_dialect)
        annotated_query_tree = annotate_types(query_tree)
        query = annotated_query_tree.sql(dialect="trino")

        # Perform custom transformations using SQLGlot's parsed representation
        if sqlglot_dialect == "spark":
            query_tree = spark_transforms(query)
            if syntax_only:
                raise ValueError("the `syntax_only` flag does not apply for Spark queries")
        elif sqlglot_dialect == "postgres":
            # Update bytearray syntax
            query = query.replace("\\x", "0x")
            query_tree = postgres_transforms(query)
            if not syntax_only:
                query_tree = v1_tables_to_v2_tables(query_tree, dataset)

        # Output the query as DuneSQL
        query = query_tree.sql(dialect=DuneSQL, pretty=True)

        # Replace placeholders with Dune params again
        for replace, original in parameter_map.items():
            query = query.replace(replace, original)

        # Non-SQLGlot transforms
        query = fix_bytearray_param(query)

        return add_warnings_and_banner(query)

    except ParseError as e:
        # SQLGlot inserts terminal style colors to emphasize error location.
        # We remove these, as they mess up the formatting.
        # Also, don't leak intermediate param syntax in error message
        error_message = (
            str(e)
            .replace("\x1b[4m", "")
            .replace("\x1b[0m", "")
            .replace(double_quoted_param_left_placeholder, "{{")
            .replace(double_quoted_param_right_placeholder, "}}")
        )
        # Remove Line and Column information, since it's outdated due to previous transforms.
        error_message = re.sub(
            ". Line [0-9]+, Col: [0-9]+.",
            ".",
            error_message,
        )
        raise DuneTranslationError(error_message)
