from pathlib import Path


def canonicalize(multiline_string):
    """Return a canonical version of the multiline string, with lowercase text and without whitespace"""
    return " ".join(line.strip() for line in multiline_string.split("\n")).lower()


def assert_output(output, expected_output):
    clean_output = canonicalize(output)
    if clean_output != expected_output:
        print("== got ==")
        print(clean_output)
        print()
        print("== expected ==")
        print(expected_output)
    assert clean_output == expected_output


def read_test_case(testcase):
    p = Path(__file__).parent
    in_filename = p / testcase.in_filename
    out_filename = p / testcase.out_filename
    with open(in_filename, "r") as f:
        query = f.read()
    with open(out_filename, "r") as f:
        expected_output = canonicalize(f.read())
    return query, expected_output
