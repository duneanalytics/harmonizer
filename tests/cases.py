from dataclasses import dataclass


@dataclass
class PostgresTestCase:
    in_filename: str
    out_filename: str
    dataset: str


postgres_test_cases = [
    PostgresTestCase("test_cases/postgres/dex.in", "test_cases/postgres/dex_ethereum.out", "ethereum"),
    PostgresTestCase("test_cases/postgres/dex.in", "test_cases/postgres/dex_polygon.out", "polygon"),
    PostgresTestCase("test_cases/postgres/aliases.in", "test_cases/postgres/aliases.out", "ethereum"),
    PostgresTestCase("test_cases/postgres/now.in", "test_cases/postgres/now.out", "ethereum"),
    PostgresTestCase("test_cases/postgres/bytea.in", "test_cases/postgres/bytea.out", "ethereum"),
    PostgresTestCase("test_cases/postgres/bytea2numeric.in", "test_cases/postgres/bytea2numeric.out", "ethereum"),
    PostgresTestCase("test_cases/param.in", "test_cases/param.out", "ethereum"),
]


@dataclass
class SparkTestCase:
    in_filename: str
    out_filename: str


spark_test_cases = [
    SparkTestCase("test_cases/spark/interval.in", "test_cases/spark/interval.out"),
    SparkTestCase("test_cases/spark/matic.in", "test_cases/spark/matic.out"),
    SparkTestCase("test_cases/spark/bytea_param.in", "test_cases/spark/bytea_param.out"),
    SparkTestCase("test_cases/spark/bytea_lower.in", "test_cases/spark/bytea_lower.out"),
    SparkTestCase("test_cases/param.in", "test_cases/param.out"),
    SparkTestCase("test_cases/spark/quoted_column.in", "test_cases/spark/quoted_column.out"),
    SparkTestCase("test_cases/spark/0x_strings.in", "test_cases/spark/0x_strings.out"),
    SparkTestCase("test_cases/spark/interval_week.in", "test_cases/spark/interval_week.out"),
    SparkTestCase("test_cases/spark/timestamp.in", "test_cases/spark/timestamp.out"),
    SparkTestCase("test_cases/spark/params.in", "test_cases/spark/params.out"),
    SparkTestCase("test_cases/spark/explode.in", "test_cases/spark/explode.out"),
    SparkTestCase("test_cases/spark/bytea2numeric_0x.in", "test_cases/spark/bytea2numeric_0x.out"),
    SparkTestCase("test_cases/spark/array_index.in", "test_cases/spark/array_index.out"),
]


@dataclass
class NLQTestCase:
    in_filename: str
    out_filename: str
    dataset: str


postgres_cases_to_remove = [
    PostgresTestCase("test_cases/postgres/dex.in", "test_cases/postgres/dex_ethereum.out", "ethereum"),
    PostgresTestCase("test_cases/postgres/dex.in", "test_cases/postgres/dex_polygon.out", "polygon"),
]
nlq_test_cases = [case for case in postgres_test_cases if case not in postgres_cases_to_remove]
