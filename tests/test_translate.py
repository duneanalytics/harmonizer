import pytest

from dune.harmonizer import translate_postgres, translate_spark
from tests.cases import nlq_test_cases, postgres_test_cases, spark_test_cases
from tests.helpers import canonicalize, read_test_case


@pytest.mark.parametrize("test_case", postgres_test_cases)
def test_translate_postgres(test_case):
    query, expected_output = read_test_case(test_case)
    output = translate_postgres(query=query, dataset=test_case.dataset)
    assert canonicalize(output) == expected_output


@pytest.mark.parametrize("test_case", spark_test_cases)
def test_translate_spark(test_case):
    query, expected_output = read_test_case(test_case)
    output = translate_spark(query=query)
    assert canonicalize(output) == expected_output


@pytest.mark.parametrize("test_case", nlq_test_cases)
def test_translate_nlq(test_case):
    query, expected_output = read_test_case(test_case)
    output = translate_postgres(query=query, dataset=None, syntax_only=True)
    assert canonicalize(output) == expected_output


def test_translate_with_mapping():
    query = "SELECT * FROM tbl, aave.AaveCollateralVaultProxy_evt_DeployVault"
    expected_output = "SELECT * FROM new_tbl, aave_ethereum.AaveCollateralVaultProxy_evt_DeployVault"
    output = translate_postgres(
        query=query,
        dataset="ethereum",
        syntax_only=False,
        table_mapping={
            "tbl": "new_tbl",
            "aave.AaveCollateralVaultProxy_evt_DeployVault": "aave_ethereum.AaveCollateralVaultProxy_evt_DeployVault"
        },
    )
    assert canonicalize(expected_output) in canonicalize(output)
