import re
from functools import partial

import sqlglot


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
                if re.search(r"^\(\s*select\b", substring, re.IGNORECASE):  # if substring starts with "(select " then
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


# SQLGlot functions


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
quoted_param_left_placeholder = f'"{param_left_placeholder}'
quoted_param_right_placeholder = f'{param_right_placeholder}"'


def interval_fix(node):
    """handle interval syntax change from Spark to Trino"""
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
        identifier = " ".join(regex_matches)
        value, granularity, *rest = identifier.split()
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
            if granularity[-1] == "s":
                granularity = granularity[:-1]
            if granularity == "week":  # we don't have week in Trino SQL
                value = int(value) * 7
                granularity = "day"
                rest.append("--week doesnt work in DuneSQL\n")
            final_interval = (
                ("INTERVAL '" + str(value) + "' " + granularity + " ".join(rest))
                .replace(param_left_placeholder, quoted_param_left_placeholder)
                .replace(param_right_placeholder, quoted_param_right_placeholder)
            )
            return sqlglot.parse_one(final_interval, read="trino")
    return node


def bytearray_parameter_fix(node):
    """Take care of parameters that use bytearrays"""
    if node.key == "eq":
        if all(
            a_param in node.sql(dialect="trino").lower()
            for a_param in [
                "0x",
                "substring(",
                quoted_param_left_placeholder,
                quoted_param_right_placeholder,
            ]
        ):
            # include param_left variables in regex pattern
            pattern = (
                r".*SUBSTRING\(\s*['\"]"
                + re.escape(quoted_param_left_placeholder)
                + r"(.*?)"
                + re.escape(quoted_param_right_placeholder)
                + r"['\"].*?\)"
            )
            match = re.search(pattern, node.sql(dialect="trino"))
            return sqlglot.parse_one(
                node.sql(dialect="trino").split("=")[0] + '= lower("{{' + match.group(1) + '}}")', read="trino"
            )
    return node


def table_replacement(node):
    """Table replacement logic for changes in schemas. only needed for migrations from Postgres."""
    if (
        isinstance(node, sqlglot.exp.Table)
        and quoted_param_left_placeholder not in node.sql(dialect="trino")
        and quoted_param_right_placeholder not in node.sql(dialect="trino")
    ):  # not a parameterized table name
        full_table_node = node.sql(dialect="trino").replace('"', "")

        # some spells require more custom mapping
        spellbook = {
            "erc20.ERC20_evt_Transfer": "erc20_ethereum.evt_Transfer",
            "bep20.BEP20_evt_Transfer": "erc20_bnb.evt_Transfer",
            "erc721.ERC721_evt_Transfer": "erc721_ethereum.evt_Transfer",
            "bep20.tokens": "tokens.erc20",
            "erc20.tokens": "tokens.erc20",
            "erc721.tokens": "tokens.nft",
            "prices.layer1_usd_btc": "prices.usd",
            "prices.layer1_usd_eth": "prices.usd",
        }
        if any(spell in full_table_node for spell in list(spellbook.keys())):
            # requires extra where statement for blockchain too for dex.in.trades and nft.trades
            spell_table = spellbook[re.split("as", full_table_node, flags=re.IGNORECASE)[0].strip()]

            if node.unalias().alias is not None:
                # if an alias is used for the table, add it back
                spell_table = spell_table + " as " + node.unalias().alias
            return sqlglot.parse_one(spell_table, read="trino")

        # else if decoded table, then add _ethereum to the table name
        elif any(decoded in node.name for decoded in ["_evt_", "_call_"]):
            # add _ethereum to all decoded tables
            chain_added_table = full_table_node.split(".")[0] + "_ethereum." + node.name  # depends on engine id
            if node.unalias().alias is not None:
                # if an alias is used, add it back on
                chain_added_table = chain_added_table + " as " + node.unalias().alias
            return sqlglot.parse_one(chain_added_table, read="trino")

    # otherwise it's some unknown/CTE, so we don't change anything
    return node


def cast_numeric(node):
    """if a column is being added, subtracted, multiplied, divided, etc,
    and it has amount/value in the name, cast to double"""
    if node.key == "column":
        if any(val in node.name.lower() for val in ["amount", "value"]):
            return sqlglot.parse_one("cast(" + node.name + " as double)", read="trino")
    return node


def cast_timestamp(node):
    if node.key == "literal":
        # and contains 'yyyy-mm-dd' format then cast to timestamp
        if re.search(r"\d{4}-\d{2}-\d{2}", node.sql(dialect="trino")):
            return sqlglot.parse_one("timestamp " + node.sql(dialect="trino"), read="trino")

        # or if it is a param that contains date/time
        pattern = re.escape(quoted_param_left_placeholder) + r"(.*?)" + re.escape(quoted_param_right_placeholder)
        match = re.search(pattern, node.sql(dialect="trino"))
        if match and any(d in node.sql(dialect="trino").lower() for d in ["date", "time"]):
            return sqlglot.parse_one("timestamp '{{" + match.group(1) + "}}'", read="trino")
    return node


def fix_boolean(node):
    """If node.key is 'literal' and contains 'true' or 'false' then cast to boolean"""
    if node.key == "literal":
        if any(boolean in node.sql(dialect="trino").lower() for boolean in ["true", "false"]):
            # remove single or double quotes
            bool_cleaned = node.sql(dialect="trino").replace('"', "").replace("'", "")
            return sqlglot.parse_one(bool_cleaned, read="trino")
    return node


def warn_unnest(node):
    """If there is an unnest function call, add a warning to the top of the query"""
    if node.name.lower() in ["unnest", "explode"]:
        return sqlglot.parse_one(
            node.sql(dialect="trino")
            + (
                "-- WARNING: You can't use explode/unnest inside SELECT anymore, it must be LATERAL "
                + "or CROSS JOIN instead. Check out the docs here: https://dune.com/docs/reference/dune-v2/query-engine"
            ),
            read="trino",
        )
    return node


def warn_sequence(node):
    """If the query uses generate_series/sequence, add a warning that links to docs"""
    if node.name.lower() in ["generate_series", "sequence"]:
        return sqlglot.parse_one(
            node.sql(dialect="trino")
            + (
                "-- WARNING: Check out the docs for example of time series generation: "
                + "https://dune.com/docs/reference/dune-v2/query-engine/ "
            ),
            read="trino",
        )
    return node


def prep_query(query, dialect):
    # sqlglot can't parse {{ }} well, so we replace with a placeholder
    query = query.replace("{{", quoted_param_left_placeholder).replace("}}", quoted_param_right_placeholder)

    # updating quote bytearrays, not bothering with removing quotes or lower()
    query = query.replace("\\x", "0x")

    function_keywords = ["replace"]

    for keyword in function_keywords:
        # use regex to replace the keyword with quotes around it
        query = re.sub(
            r"\b" + re.escape(keyword) + r"(?!\()",
            '"' + keyword + '"',
            query,
            flags=re.IGNORECASE,
        )

    res = sqlglot.transpile(query, read=dialect, write="trino", pretty=True)[0]
    expression_tree = sqlglot.parse_one(res, read="trino")
    return expression_tree


def spell_rename(query):
    """Rename a column in a spell, there might be more of these"""
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


def transforms(query_tree, dialect, dataset):
    """Apply several transforms to the query in order. Each transform takes and returns a sqlglot.Expression"""
    if dialect == "postgres":
        # Add an appropriate blockchain = '<chain>' filter for trades, tokens, and prices tables.
        chain_where = {
            "gnosis": chain_where_gnosis,
            "optimism": chain_where_optimism,
            "bnb": chain_where_bnb,
            "polygon": chain_where_polygon,
        }.get(dataset, chain_where_ethereum)
        transform_order = [
            table_replacement,  # must be first, takes care of table names and quotes
            interval_fix,
            fix_boolean,
            cast_numeric,
            cast_timestamp,
            warn_unnest,
            warn_sequence,
            dex_trades_fixes,
            chain_where,
            bytearray_parameter_fix,
            spell_rename,
            bytea2numeric,
        ]
    else:
        transform_order = [
            interval_fix,
            fix_boolean,
            cast_numeric,
            cast_timestamp,
            warn_unnest,
            warn_sequence,
            bytea2numeric,
        ]

    for f in transform_order:
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
