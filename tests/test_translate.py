import pytest

from dune.translate import migrate_postgres, migrate_spark
from tests.cases import postgres_test_cases, spark_test_cases
from tests.helpers import assert_output, read_test_case


@pytest.mark.parametrize("test_case", postgres_test_cases)
def test_translate_postgres(test_case):
    query, expected_output = read_test_case(test_case)
    output = migrate_postgres(query=query, dataset=test_case.dataset)
    assert_output(output, expected_output)


@pytest.mark.parametrize("test_case", spark_test_cases)
def test_translate_spark(test_case):
    query, expected_output = read_test_case(test_case)
    output = migrate_spark(query=query)
    assert_output(output, expected_output)
