from dune.harmonizer.translate import _clean_dataset, _translate_query


def translate_spark(query):
    """Translate a Dune query from Spark SQL to DuneSQL"""
    return _translate_query(query, sqlglot_dialect="spark")


def translate_postgres(query, dataset, syntax_only=False):
    """Translate a Dune query from PostgreSQL to DuneSQL

    By default, this will replace any known v1 to v2 differences in datasets.
    To only translate the syntax, call this with `syntax_only=True`.
    """
    dataset = _clean_dataset(dataset)
    translated = _translate_query(query, sqlglot_dialect="postgres", dataset=dataset, syntax_only=syntax_only)
    return translated
