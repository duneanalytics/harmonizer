# Harmonizer

Harmonizer is a library for translating Dune queries from PostgreSQL and Spark SQL to DuneSQL.
It currently powers our in-app migration service.

<img width="456" alt="Screenshot 2023-05-04 at 11 53 01" src="https://user-images.githubusercontent.com/5699893/236171827-577c28dd-c10c-423b-b6b0-58dca14d5497.png">

Harmonizer makes heavy use of [SQLGlot](https://github.com/tobymao/sqlglot),
an excellent tool for working with SQL queries.
With it, we parse the query into an Abstract Syntax Tree (AST),
and can manipulate the AST, and  finally generate the SQL for that query, even in a different dialect.

We add a DuneSQL dialect, and use SQLGlot to translate from Spark SQL/PostgreSQL to DuneSQL.
In the DuneSQL dialect, we translate string literals '0x...' to 0x..., since we [support native hex literals](https://dune.com/docs/query/DuneSQL-reference/datatypes/#varbinary).

Harmonizer also does a mapping of known changes in table names from the legacy Postgres datasets to corresponding table names in DuneSQL.
We need help to make this mapping more complete!

## Getting started

Install with

```
pip install dune-harmonizer
```

Now import the `translate_` functions in your code:

```python
from dune.harmonizer import translate_spark, translate_postgres
```

with function signatures

```python
def translate_spark(query: str) -> str:
    ...

def translate_postgres(query: str, dataset: str) -> str:
    ...
```

## Contributing

Contributions are very welcome!

Please open an issue or PR, and we will get back to you as soon as we can.

**If you've found a table that doesn't get mapped to one that exists on Dune SQL**, then you can open an issue or just add the table mapping [to this line](https://github.com/duneanalytics/harmonizer/blob/main/dune/harmonizer/table_replacements.py#L18) here in a PR.

**If there is a function that doesn't get mapped correctly**, then you can open an issue or try and [add one here using sqlglot](https://github.com/duneanalytics/harmonizer/blob/main/dune/harmonizer/custom_transforms.py) and open a PR.

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

We test on examples in the `test_cases` directory.
To force an update of the expected outputs, run the `update_expected_outputs` script like below

```
poetry run python tests/update_expected_outputs.py
```
