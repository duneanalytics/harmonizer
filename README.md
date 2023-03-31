# Dune Query Translator

A library we use in the Dune app to translate Dune queries from PostgreSQL and Spark SQL to DuneSQL.

A query is translated through two steps.

1. We use [SQLGlot](https://github.com/tobymao/sqlglot) to transpile the query.
2. We pass the query through some handwritten rules for additional changes.

## Getting started

Install with

```
pip install dune-query-translator
```

Now import the `translate` function in your code:

```python
from dune.translate.translate import translate
```

The function takes three arguments: A query, a dialect, and a Dune dataset name (`ethereum`, `bnb`, etc).

## Contributing

Contributions are very welcome.
Please open an issue or a PR and we will get back to you within a week.

## Development

Install with

```
poetry install
```

If the Ruff linter complains, running the following and committing the changes should suffice

```
poetry run ruff . --fix
poetry run black .
```

Run tests with

```
poetry run pytest
```

We test on examples in the `sqlglot_test_cases` directory.
To force an update of the expected outputs, do

```
poetry run python tests/update_expected_outputs.py
```
