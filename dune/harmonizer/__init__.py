from dune.harmonizer.translate import _clean_dataset, _translate_query


def translate_spark(query):
    """Translate a Dune query from Spark SQL to DuneSQL"""
    return _translate_query(query, sqlglot_dialect="spark")


def translate_postgres(query, dataset):
    """Translate a Dune query from PostgreSQL to DuneSQL"""
    dataset = _clean_dataset(dataset)
    return _translate_query(query, sqlglot_dialect="postgres", dataset=dataset)
