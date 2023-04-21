from pathlib import Path


def canonicalize(multiline_string):
    """Return a canonical version of the multiline string, with lowercase text and without whitespace"""
    return " ".join(line.strip() for line in multiline_string.split("\n")).lower()


def read_test_case(testcase):
    p = Path(__file__).parent
    in_filename = p / testcase.in_filename
    out_filename = p / testcase.out_filename
    with open(in_filename, "r") as f:
        query = f.read()
    with open(out_filename, "r") as f:
        expected_output = canonicalize(f.read())
    return query, expected_output
