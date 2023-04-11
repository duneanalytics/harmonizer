from pathlib import Path

from dune.translate.translate import translate
from tests.test_translate import test_cases


def update_test_case(tc):
    """Overrides the expected output of the test case with the output of putting the input through translate_func"""
    p = Path(__file__).parent
    in_filename = p / tc.in_filename
    out_filename = p / tc.out_filename
    with open(in_filename, "r") as f:
        query = f.read()
    output = translate(query, tc.dialect, tc.dataset)
    with open(out_filename, "w") as f:
        f.write(output)


if __name__ == "__main__":
    for testcase in test_cases:
        update_test_case(testcase)
