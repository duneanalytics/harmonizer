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
]
