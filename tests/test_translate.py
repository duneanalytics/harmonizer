from dataclasses import dataclass
from pathlib import Path

import pytest

from dune.translate.translate import translate


@dataclass
class Case:
    in_filename: str
    out_filename: str
    dialect: str
    dataset: str  # can be empty if not relevant to query


test_cases_sqlglot = [
    Case("sqlglot_test_cases/dex.in", "sqlglot_test_cases/dex.out", "postgres", "ethereum"),
    Case("sqlglot_test_cases/interval.in", "sqlglot_test_cases/interval.out", "spark", ""),
    Case("sqlglot_test_cases/matic.in", "sqlglot_test_cases/matic.out", "spark", ""),
    Case("sqlglot_test_cases/param.in", "sqlglot_test_cases/param.out", "postgres", ""),
]


def canonicalize(multiline_string):
    """Return a canonical version of the multiline string, with lowercase text and without whitespace"""
    return " ".join(line.strip() for line in multiline_string.split("\n")).lower()


@pytest.mark.parametrize("testcase", test_cases_sqlglot)
def test_translate_sqlglot(testcase):
    p = Path(__file__).parent
    in_filename = p / testcase.in_filename
    out_filename = p / testcase.out_filename
    with open(in_filename, "r") as f:
        query = f.read()
    with open(out_filename, "r") as f:
        expected_output = canonicalize(f.read())
    output = translate(query=query, dialect=testcase.dialect, dataset=testcase.dataset)
    clean_output = canonicalize(output)
    if clean_output != expected_output:
        print("== got ==")
        print(output)
        print()
        print("== expected ==")
        print(expected_output)
    assert clean_output == expected_output
