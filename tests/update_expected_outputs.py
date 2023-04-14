from pathlib import Path

from dune.translate import migrate_spark, migrate_postgres
from tests.cases import postgres_test_cases, SparkTestCase, PostgresTestCase, spark_test_cases


def update_test_case(tc):
    """Overrides the expected output of the test case with the output of putting the input through translate_func"""
    p = Path(__file__).parent
    in_filename = p / tc.in_filename
    out_filename = p / tc.out_filename
    with open(in_filename, "r") as f:
        query = f.read()
    if isinstance(tc, SparkTestCase):
        output = migrate_spark(query)
    elif isinstance(tc, PostgresTestCase):
        output = migrate_postgres(query, tc.dataset)
    with open(out_filename, "w") as f:
        f.write(output)


if __name__ == "__main__":
    for testcase in postgres_test_cases:
        update_test_case(testcase)
    for testcase in spark_test_cases:
        update_test_case(testcase)
