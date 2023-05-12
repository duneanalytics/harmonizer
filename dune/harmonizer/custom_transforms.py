import re
from functools import partial

import sqlglot

from dune.harmonizer.table_replacements import postgres_table_replacements


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
        "dex.trades",
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
        if "dex.trades" in node.sql(dialect="trino").replace('"', ""):
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


param_left_placeholder = "left_param_"
param_right_placeholder = "_right_param"
double_quoted_param_left_placeholder = f'"{param_left_placeholder}'
double_quoted_param_right_placeholder = f'{param_right_placeholder}"'
single_quoted_param_left_placeholder = f"'{param_left_placeholder}"
single_quoted_param_right_placeholder = f"{param_right_placeholder}'"


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
    """if a column has amount/value in the name, cast to double"""
    if node.key == "column":
        if any(val in node.name.lower() for val in ("amount", "value")):
            return sqlglot.parse_one("cast(" + node.name + " as double)", read="trino")
    return node


def cast_timestamp_parameters(node):
    """Look for parameters with 'date' or 'time' in, and cast these as timestamps"""
    if node.key == "literal":
        # or if it is a param that contains date/time
        pattern = "('" + param_left_placeholder + r".*?" + param_right_placeholder + "')"
        if any(d in node.sql(dialect="trino").lower() for d in ("date", "time")):
            q = re.sub(pattern, r"timestamp \1", node.sql(dialect="trino"))
            return sqlglot.parse_one(q, read="trino")
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


def rename_amount_column(query):
    """Rename the usd_amount column"""
    return sqlglot.parse_one(query.sql(dialect="trino").replace("usd_amount", "amount_usd"), read="trino")


def fix_bytearray_param(query):
    """Remove lower function call from bytearray parameters"""
    pattern = r"lower\(\s*['\"]\{\{(.*?)\}\}['\"]\s*\)"
    return re.sub(pattern, r"{{\1}}", query, flags=re.IGNORECASE)


def chain_where(dataset):
    return {
        "gnosis": chain_where_gnosis,
        "optimism": chain_where_optimism,
        "bnb": chain_where_bnb,
        "polygon": chain_where_polygon,
        "ethereum": chain_where_ethereum,
    }[dataset]


def explicit_alias_on_cast(query_tree):
    """In Postgres, a simple cast of a column will retain the column name, so we add an explicit cast"""
    return query_tree.transform(
        lambda e: sqlglot.exp.Alias(this=e, alias=e.alias_or_name)
        if isinstance(e, sqlglot.exp.Cast)
        and isinstance(e.this, sqlglot.exp.Column)
        and isinstance(e.parent, sqlglot.exp.Select)
        else e
    )


def postgres_transforms(query):
    """Apply a series of transforms to the query tree, recursively using SQLGlot's recursive transform function.

    Each transform takes and returns a sqlglot.Expression"""
    query_tree = sqlglot.parse_one(query, read="trino")
    transforms = (
        cast_numeric,
        cast_timestamp_parameters,
        warn_sequence,
        bytearray_parameter_fix,
        explicit_alias_on_cast,
    )
    for f in transforms:
        query_tree = query_tree.transform(f)
    return query_tree


def v1_tables_to_v2_tables(query_tree, dataset):
    """Apply a series of transforms to the query tree, recursively using SQLGlot's recursive transform function.

    Each transform takes and returns a sqlglot.Expression

    These transforms are concerned with translating from the v1 tables in Postgres datasets to the v2 tables"""
    transforms = (
        postgres_table_replacements(dataset),
        dex_trades_fixes,
        chain_where(dataset),
        rename_amount_column,
    )
    for f in transforms:
        query_tree = query_tree.transform(f)
    return query_tree


def spark_transforms(query):
    """Apply a series of transforms to the query tree, recursively using SQLGlot's recursive transform function.

    Each transform takes and returns a sqlglot.Expression"""
    query_tree = sqlglot.parse_one(query, read="trino")
    transforms = (
        cast_numeric,
        cast_timestamp_parameters,
        warn_sequence,
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


def parameter_placeholder(p):
    return (
        p.replace("{{", param_left_placeholder)
        .replace("}}", param_right_placeholder)
        .replace(" ", "_")
        .replace("-", "_")
        .lower()
    )
