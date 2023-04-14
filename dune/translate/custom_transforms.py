import re
from functools import partial

import sqlglot

from dune.translate.table_replacements import postgres_table_replacements


def extract_nested_select(text):
    stack = []
    results = []
    for i, char in enumerate(text):
        if char == "(":
            stack.append(i)
        elif char == ")":
            if len(stack) > 1:
                stack.pop()  # if a ")" is found but stack is greater than 1, we're still in a nested select
            else:  # if stack is 1, we're at the end of the nested select
                end = i + 1
                substring = text[stack[0] : end].strip()
                stack = []  # reset the stack after an end is found
                # if substring starts with "(select "
                if re.search(r"^\(\s*select\b", substring, re.IGNORECASE):
                    results.append(substring)
    return results


def recurse_where(node, required_tables, condition_add):
    """
    we can't just iterate through the tree because of weird replace node behaviors, so this iterates
    through SELECT statements and specifically adds WHERE statements with a specific condition_add
    for a given set of required_tables, then goes through each removed select recursively to do the same thing

    Useful for adding aliased "blockchain = 'ethereum'"  or other where statements in the right places.
    """
    if isinstance(node, sqlglot.Expression):
        statement = node.sql(dialect="trino")
    elif isinstance(node, str):
        statement = node
    else:
        raise ValueError(f"node must be a sqlglot.Expression or string, not {type(node)}")
    condition_add = condition_add.strip()  # remove whitespace

    # we're going to remove inner selects, add WHERE statements, then put them back in.
    match_groups = extract_nested_select(statement)

    placeholder = "(SELECT * FROM __PLACEHOLDER"
    for i, group in enumerate(match_groups):
        statement = statement.replace(group, f"{placeholder}{i}__)")

    tables = sqlglot.parse_one(statement, read="trino").find_all(sqlglot.exp.Table)
    for table in tables:
        for req in required_tables:
            if req in str(table).replace('"', ""):
                if len(table.alias) > 0:
                    statement_to_add = " where " + table.alias + "." + condition_add
                else:
                    statement_to_add = " where " + condition_add

                # check if there is already a where statement
                where_statement = re.split("where", statement, flags=re.IGNORECASE)

                if len(where_statement) > 1:
                    statement = re.sub(
                        "where",
                        statement_to_add + " and ",
                        statement,
                        flags=re.IGNORECASE,
                    )  # add to front of statement
                else:
                    # add statement_to_add before the last relevant keyword in statement
                    if "group by" in statement:
                        statement = re.sub(
                            "group",
                            statement_to_add + " group ",
                            statement,
                            flags=re.IGNORECASE,
                        )
                    elif "order by" in statement:
                        statement = re.sub(
                            "order",
                            statement_to_add + " order ",
                            statement,
                            flags=re.IGNORECASE,
                        )
                    elif "limit" in statement:
                        statement = re.sub(
                            "limit",
                            statement_to_add + " limit ",
                            statement,
                            flags=re.IGNORECASE,
                        )
                    elif "offset" in statement:
                        statement = re.sub(
                            "offset",
                            statement_to_add + " offset ",
                            statement,
                            flags=re.IGNORECASE,
                        )
                    else:
                        # add to end of statement if none of the above keywords exist
                        statement = statement + statement_to_add

    # if nothing to replace anymore, return
    if len(match_groups) == 0:
        return statement

    # if there are still placeholders to recurse through
    for i, group in enumerate(match_groups):
        # remove the outer parens so it isn't caught in infinite single recursion
        parsed_group = recurse_where(group[1:-1], required_tables, condition_add)
        statement = statement.replace(f"{placeholder}{i}__)", "(" + parsed_group + ")")

    return statement


def chain_where_blockchain(node, blockchain):
    # add a blockchain = 'ethereum' to the WHERE statement for trades, tokens, and prices tables.
    required_tables = [
        "nft.trades",
        "dex.in.trades",
        "tokens.erc20",
        "tokens.nft",
        "prices.usd",
    ]
    condition_add = f"blockchain = '{blockchain}'"

    # only run this for the highest level select, then it will recurse down
    if node.key == "select" and node.parent is None:
        statement = recurse_where(node, required_tables, condition_add)  # function defined above
        return sqlglot.parse_one(statement, read="trino")
    return node


chain_where_ethereum = partial(chain_where_blockchain, blockchain="ethereum")
chain_where_gnosis = partial(chain_where_blockchain, blockchain="gnosis")
chain_where_optimism = partial(chain_where_blockchain, blockchain="optimism")
chain_where_bnb = partial(chain_where_blockchain, blockchain="bnb")
chain_where_polygon = partial(chain_where_blockchain, blockchain="polygon")


def dex_trades_fixes(node):
    """Fixes to dex.trades"""
    # doesn't matter if subquery or not, it will replace the found filter.
    if node.key == "select":
        if "dex.in.trades" in node.sql(dialect="trino").replace('"', ""):
            # change exchange_contract_address to project_contract_address
            final_where = node.sql(dialect="trino").replace("exchange_contract_address", "project_contract_address")

            # removing category from WHERE statement since that isn't in dex.in.trades anymore
            clean_matches = [
                # uses alias
                r'(?i)\w+\.[\'"]?category[\'"]?\s*=\s*[\'"]dex.in[\'"]\s*and',  # there is a following AND
                r'(?i)and\s*\w+\.[\'"]?category[\'"]?\s*=\s*[\'"]dex.in[\'"]',  # there is a preceding AND
                # it is the only thing in the WHERE statement doesn't use alias
                r'(?i)where\s*\w+\.[\'"]?category[\'"]?\s*=\s*[\'"]dex.in[\'"]',
                r'(?i)[\'"]?category[\'"]?\s*=\s*[\'"]dex.in[\'"]\s*and',  # there is a following AND
                r'(?i)and\s*[\'"]?category[\'"]?\s*=\s*[\'"]dex.in[\'"]',  # there is a preceding AND
                # it is the only thing in the WHERE statement
                r'(?i)where\s*[\'"]?category[\'"]?\s*=\s*[\'"]dex.in[\'"]',
            ]

            for match in clean_matches:
                if re.search(match, final_where):
                    # if category = 'DEX' is the only thing in the WHERE statement, remove the WHERE statement
                    final_where = re.sub(match, "", final_where, flags=re.IGNORECASE)

            # fix token columns
            final_where = re.sub(
                "token_a_address",
                "token_sold_address",
                final_where,
                flags=re.IGNORECASE,
            )
            final_where = re.sub(
                "token_b_address",
                "token_bought_address",
                final_where,
                flags=re.IGNORECASE,
            )

            return sqlglot.parse_one(final_where, read="trino")
    return node


param_left_placeholder = "parameter_placeholder_left_bracket"
param_right_placeholder = "parameter_placeholder_right_bracket"
double_quoted_param_left_placeholder = f'"{param_left_placeholder}'
double_quoted_param_right_placeholder = f'{param_right_placeholder}"'
single_quoted_param_left_placeholder = f"'{param_left_placeholder}"
single_quoted_param_right_placeholder = f"{param_right_placeholder}'"


# TODO: Remove or simplify once the fix to https://github.com/tobymao/sqlglot/issues/1410 is released
def interval_fix(node):
    """Handle interval syntax change from Spark to Trino"""
    if node.key == "interval":
        # node.this is the argument to the interval function, possibly a parenthesized expression
        interval_argument = str(node.this)
        if interval_argument[0] == "(" and interval_argument[-1] == ")":
            interval_argument = interval_argument[1:-1]
        # Split on quotes
        regex_split = re.split(r'[\'"]', interval_argument)
        # remove empty elements (will occur if there are quotes inside the interval value)
        # matches start at index 1, index 0 is the original string
        regex_matches = [i for i in regex_split[1:] if i != ""]
        # no match, return
        if len(regex_matches) == 0:
            return node
        interval_argument = " ".join(regex_matches)
        if len(interval_argument.split()) == 1:
            return node
        value, granularity, *rest = interval_argument.split()
        if any(
            known_granularity in granularity.lower()
            for known_granularity in [
                "second",
                "minute",
                "hour",
                "day",
                "week",
                "month",
                "year",
            ]
        ):
            if granularity.endswith('s'):
                granularity = granularity[:-1]
            if granularity == "week":  # we don't have week in Trino SQL
                value = int(value) * 7
                granularity = "day"
                rest.append("--week doesn't work in DuneSQL\n")
            final_interval = (
                ("INTERVAL '" + str(value) + "' " + granularity + " ".join(rest))
                .replace(param_left_placeholder, double_quoted_param_left_placeholder)
                .replace(param_right_placeholder, double_quoted_param_right_placeholder)
            )
            return sqlglot.parse_one(final_interval, read="trino")
    return node


def bytearray_parameter_fix(node):
    """Take care of parameters that use bytearrays"""
    if node.key == "eq":
        if all(
            a_param in node.sql(dialect="trino").lower()
            for a_param in (
                "0x",
                "substring(",
                double_quoted_param_left_placeholder,
                double_quoted_param_right_placeholder,
            )
        ):
            # include param_left variables in regex pattern
            pattern = (
                r".*SUBSTRING\(\s*['\"]"
                + re.escape(double_quoted_param_left_placeholder)
                + r"(.*?)"
                + re.escape(double_quoted_param_right_placeholder)
                + r"['\"].*?\)"
            )
            match = re.search(pattern, node.sql(dialect="trino"))
            return sqlglot.parse_one(
                node.sql(dialect="trino").split("=")[0] + '= lower("{{' + match.group(1) + '}}")', read="trino"
            )
    return node


def cast_numeric(node):
    """if a column is being added, subtracted, multiplied, divided, etc,
    and it has amount/value in the name, cast to double"""
    if node.key == "column":
        if any(val in node.name.lower() for val in ("amount", "value")):
            return sqlglot.parse_one("cast(" + node.name + " as double)", read="trino")
    return node


def cast_timestamp(node):
    if node.key == "literal":
        # and contains 'yyyy-mm-dd' format then cast to timestamp
        if re.search(r"\d{4}-\d{2}-\d{2}", node.sql(dialect="trino")):
            return sqlglot.parse_one("timestamp " + node.sql(dialect="trino"), read="trino")

        # or if it is a param that contains date/time
        pattern = (
            re.escape(double_quoted_param_left_placeholder)
            + r"(.*?)"
            + re.escape(double_quoted_param_right_placeholder)
        )
        match = re.search(pattern, node.sql(dialect="trino"))
        if match and any(d in node.sql(dialect="trino").lower() for d in ["date", "time"]):
            return sqlglot.parse_one("timestamp '{{" + match.group(1) + "}}'", read="trino")
    return node


def fix_boolean(node):
    """If node.key is 'literal' and contains 'true' or 'false' then cast to boolean"""
    if node.key == "literal":
        if any(boolean in node.sql(dialect="trino").lower() for boolean in ("true", "false")):
            # remove single or double quotes
            bool_cleaned = node.sql(dialect="trino").replace('"', "").replace("'", "")
            return sqlglot.parse_one(bool_cleaned, read="trino")
    return node


def warn_unnest(node):
    """Add a warning to the query if there is an unnest function call"""
    if node.name.lower() in ("unnest", "explode"):
        return sqlglot.parse_one(
            node.sql(dialect="trino")
            + (
                "-- WARNING: You can't use explode/unnest inside SELECT anymore, it must be LATERAL "
                + "or CROSS JOIN instead. Check out the docs here: https://dune.com/docs/query/syntax-differences/"
            ),
            read="trino",
        )
    return node


def warn_sequence(node):
    """Add a warning that links to docs if the query uses generate_series/sequence"""
    if node.name.lower() in ("generate_series", "sequence"):
        return sqlglot.parse_one(
            node.sql(dialect="trino")
            + (
                "-- WARNING: Check out the docs for example of time series generation: "
                "https://dune.com/docs/query/syntax-differences/"
            ),
            read="trino",
        )
    return node


def prep_query(query):
    for keyword in ["replace"]:
        # use regex to replace the keyword with quotes around it
        query = re.sub(
            r"\b" + re.escape(keyword) + r"(?!\()",
            '"' + keyword + '"',
            query,
            flags=re.IGNORECASE,
        )
    return query


def rename_amount_column(query):
    """Rename the usd_amount column"""
    return sqlglot.parse_one(query.sql(dialect="trino").replace("usd_amount", "amount_usd"), read="trino")


def bytea2numeric(query_tree):
    """Replace and warn about bytearray functions"""
    query = query_tree.sql(dialect="trino")
    if "bytea2numeric" in query.lower():
        query = re.sub("bytea2numeric", "bytearray_to_bigint", query, flags=re.IGNORECASE)
        query = (
            "/* !Bytea warning: We now have new bytearray functions to cover conversions and stuff like "
            "length, concat, substring, etc. Check out the docs here: "
            "https://dune.com/docs/reference/dune-v2/query-engine/#byte-array-to-numeric-functions */"
            "\n\n"
        ) + query
    return sqlglot.parse_one(query, read="trino")


def fix_bytearray_param(query):
    """Remove lower function call from bytearray parameters"""
    pattern = r"lower\(\s*['\"]\{\{(.*?)\}\}['\"]\s*\)"
    return re.sub(pattern, r"{{\1}}", query, flags=re.IGNORECASE)


def fix_bytearray_lower(query):
    """Remove lower function call around '0x...' string literals, and remove the string since we have native hex types.

    This has to happen after SQLGlot, since it will parse a bare 0x as a string literal"""
    pattern = r"lower\(\s*['\"]0x(.*?)['\"]\s*\)"
    substituted = re.sub(pattern, r"0x\1", query, flags=re.IGNORECASE)
    return substituted


def chain_where(dataset):
    return {
        "gnosis": chain_where_gnosis,
        "optimism": chain_where_optimism,
        "bnb": chain_where_bnb,
        "polygon": chain_where_polygon,
        "ethereum": chain_where_ethereum,
    }[dataset]


def postgres_transforms(query, dataset):
    """Apply a series of transforms to the query tree, recursively using SQLGlot's recursive transform function.

    Each transform takes and returns a sqlglot.Expression"""
    query_tree = sqlglot.parse_one(query, read="postgres")
    transforms = (
        postgres_table_replacements(dataset),
        interval_fix,
        fix_boolean,
        cast_numeric,
        cast_timestamp,
        warn_unnest,
        warn_sequence,
        dex_trades_fixes,
        chain_where(dataset),
        bytearray_parameter_fix,
        rename_amount_column,
        bytea2numeric,
    )
    for f in transforms:
        query_tree = query_tree.transform(f)
    return query_tree


def spark_transforms(query):
    """Apply a series of transforms to the query tree, recursively using SQLGlot's recursive transform function.

    Each transform takes and returns a sqlglot.Expression"""
    query_tree = sqlglot.parse_one(query, read="trino")
    transforms = (
        interval_fix,
        fix_boolean,
        cast_numeric,
        cast_timestamp,
        warn_unnest,
        warn_sequence,
        bytea2numeric,
    )
    for f in transforms:
        query_tree = query_tree.transform(f)
    return query_tree


def add_warnings_and_banner(query):
    """Add a success banner at the top, and look for a few cases of things we don't fix and add a warning if present"""
    if "lower('{{" in query.lower():
        query = (
            "/* !Bytea parameter warning: Make sure to change \\x to 0x in the parameters, bytea types are "
            "native now (no need for quotes or lower or \\x)' */"
            "\n\n"
        ) + query

    # if brackets [ ] are used, warn about array indexing
    if re.search(r"\[.*\]", query):
        query = (
            "/* !Array warning: Arrays in dune SQL are indexed from 1, not 0. "
            "The migrator will not catch this if you indexed using variables */"
            "\n\n"
        ) + query

    if "dune_user_generated" in query.lower():
        query = (
            "/* !Generated view warning: you can't query views in dune_user_generated anymore. "
            "All queries in DuneSQL are by default views though (try querying the table 'query_1747157') */"
            "\n\n"
        ) + query

    # add note at top
    return (
        "/* Success! If you're still running into issues, check out https://dune.com/docs/query/syntax-differences/ "
        "or reach out in the #dune-sql Discord channel. */"
        "\n\n"
    ) + query
