import sqlglot


def postgres_table_replacements(dataset):
    """Return a function to do table replacements for Postgres -> DuneSQL, with appropriate dataset"""

    def table_replacement_transform(table_node):
        """Replace table names in the query AST with the appropriate DuneSQL table names"""

        # We only want to do something here if we've recursed all the way to a table node in the query AST
        if not isinstance(table_node, sqlglot.exp.Table):
            return table_node

        spellbook_mapping = {
            ("erc20", "erc20_evt_transfer"): (f"erc20_{dataset}", "evt_Transfer"),
            ("bep20", "bep20_evt_transfer"): ("erc20_bnb", "evt_Transfer"),
            ("erc721", "erc721_evt_transfer"): ("erc721_ethereum", "evt_Transfer"),
            ("bep20", "tokens"): ("tokens", "erc20"),
            ("erc20", "tokens"): ("tokens", "erc20"),
            ("erc721", "tokens"): ("tokens", "nft"),
            ("prices", "layer1_usd_btc"): ("prices", "usd"),
            ("prices", "layer1_usd_eth"): ("prices", "usd"),
        }
        table = table_node.db.lower(), table_node.name.lower()
        replacement = spellbook_mapping.get(table)
        if replacement is not None:
            to_db, to_table = replacement
            if table_node.alias != "":
                return sqlglot.parse_one(f"{to_db}.{to_table} as {table_node.alias}", read="trino")
            return sqlglot.parse_one(f"{to_db}.{to_table}", read="trino")

        # if decoded table, then add _{dataset} to the table name
        if any(decoded in table_node.name.lower() for decoded in ["_evt_", "_call_"]):
            chain_added_table = table_node.sql(dialect="trino").split(".")[0] + "_" + dataset + "." + table_node.name
            # if an alias is used, add it back
            if table_node.unalias().alias is not None:
                chain_added_table = chain_added_table + " as " + table_node.unalias().alias
            return sqlglot.parse_one(chain_added_table, read="trino")

        return table_node

    return table_replacement_transform
