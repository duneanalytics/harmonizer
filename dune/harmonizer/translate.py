import re

import sqlglot
from sqlglot import ParseError
from sqlglot.errors import SqlglotError

from dune.harmonizer.custom_transforms import (
    add_warnings,
    fix_bytearray_param,
    parameter_placeholder,
    transform_interval_cast,
    v1_tables_to_v2_tables,
    v1_transforms,
    v2_transforms,
)
from dune.harmonizer.dunesql.dunesql import DuneSQL
from dune.harmonizer.errors import DuneTranslationError
from dune.harmonizer.table_replacements import spellbook_mapping


def _clean_dataset(dataset):
    if dataset is None:
        return None
    for d in ("gnosis", "optimism", "bnb", "polygon", "ethereum"):
        if d in dataset.lower():
            return d
    raise ValueError(f"Unknown dataset: {dataset}")


def _handle_parse_error(parameter_map: dict[str, str], e: ParseError) -> str:
    # SQLGlot inserts terminal style colors to emphasize error location.
    # We remove these, as they mess up the formatting.
    # Also, don't leak intermediate param syntax in error message
    error_message = str(e)#.replace("\x1b[4m", "").replace("\x1b[0m", "")

    # Replace any placeholders in the error message with their param
    for replace, original in parameter_map.items():
        error_message = error_message.replace(replace, original)

    # Remove Line and Column information, since it's outdated due to previous transforms.
    error_message = re.sub(
        ". Line [0-9]+, Col: [0-9]+.",
        ".",
        error_message,
    )
    return error_message


def _translate_query(query, sqlglot_dialect, dataset=None, syntax_only=False, table_mapping=None):
    """Translate a query using SQLGLot plus custom rules"""
    # Insert placeholders for the parameters we use in Dune (`{{ param }}`), SQLGlot doesn't handle those
    parameters = re.findall("({{.*?}})", query, flags=re.IGNORECASE)
    parameter_map = {parameter_placeholder(p): p for p in parameters}
    for replace, original in parameter_map.items():
        query = query.replace(original, replace)

    # Update bytearray syntax for postgres:
    # SQLGlot parses x'deadbeef' as a HexString, but it doesn't parse \x as a hex string,
    # because it's just a general byte array notation. But we want to always parse it as a hex string.
    if sqlglot_dialect == "postgres":
        query = query.replace(r"'\x", "x'")
        try:
            query = transform_interval_cast(query)
        except ParseError as e:
            raise DuneTranslationError(_handle_parse_error(parameter_map, e))
        except SqlglotError as e:
            raise DuneTranslationError(str(e))

    # Parse query using SQLGlot
    try:
        query_tree = sqlglot.parse_one(query, read=sqlglot_dialect)
    except ParseError as e:
        raise DuneTranslationError(_handle_parse_error(parameter_map, e))
    except SqlglotError as e:
        raise DuneTranslationError(str(e))

    # Perform custom transformations on the AST. Transforms depend on the dataset; for legacy Postgres datasets
    # we need to do table mappings as well.
    if sqlglot_dialect == "spark":
        try:
            query_tree = v2_transforms(query_tree)
        except SqlglotError as e:
            raise DuneTranslationError(str(e))
        if syntax_only:
            raise ValueError("the `syntax_only` flag does not apply for Spark queries")
    elif sqlglot_dialect == "postgres":
        try:
            query_tree = v1_transforms(query_tree)
        except SqlglotError as e:
            raise DuneTranslationError(str(e))
        if not syntax_only:
            # Add provided table mapping to the default mapping
            mapping = spellbook_mapping(dataset)
            if table_mapping is not None:
                mapping = mapping | table_mapping
            try:
                query_tree = v1_tables_to_v2_tables(query_tree, dataset, mapping)
            except SqlglotError as e:
                raise DuneTranslationError(str(e))

    # Output the query as DuneSQL
    query = query_tree.sql(dialect=DuneSQL, pretty=True)

    # Replace placeholders with Dune params again
    for replace, original in parameter_map.items():
        query = query.replace(replace, original)

    # Non-SQLGlot transforms
    query = fix_bytearray_param(query)

    return add_warnings(query)
