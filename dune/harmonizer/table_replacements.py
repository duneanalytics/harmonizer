import re

import sqlglot

from dune.harmonizer.dialects.dunesql import DuneSQL


def postgres_table_replacements(dataset):
    """Return a function to do table replacements for Postgres -> DuneSQL, with appropriate dataset"""

    def table_replacement_transform(node):
        """Replace table names in the query AST with the appropriate DuneSQL table names"""

        # We only want to do something here if we've recursed all the way to a table node in the query AST
        if not isinstance(node, sqlglot.exp.Table):
            return node

        query = node.sql(dialect=DuneSQL)

        spellbook_mapping = {
            "erc20.ERC20_evt_Transfer": "erc20_ethereum.evt_Transfer",
            "bep20.BEP20_evt_Transfer": "erc20_bnb.evt_Transfer",
            "erc721.ERC721_evt_Transfer": "erc721_ethereum.evt_Transfer",
            "bep20.tokens": "tokens.erc20",
            "erc20.tokens": "tokens.erc20",
            "erc721.tokens": "tokens.nft",
            "prices.layer1_usd_btc": "prices.usd",
            "prices.layer1_usd_eth": "prices.usd",
        }
        if any(spell.lower() in query for spell in spellbook_mapping.keys()):
            table = re.split("as", query, flags=re.IGNORECASE)[0].strip()
            spell_table = spellbook_mapping[table]

            # if an alias is used for the table, add it back
            if node.unalias().alias is not None:
                spell_table = spell_table + " as " + node.unalias().alias
            return sqlglot.parse_one(spell_table, read=DuneSQL)

        # else if decoded table, then add _ethereum to the table name
        elif any(decoded in node.name for decoded in ["_evt_", "_call_"]):
            chain_added_table = query.split(".")[0] + "_" + dataset + "." + node.name
            # if an alias is used, add it back
            if node.unalias().alias is not None:
                chain_added_table = chain_added_table + " as " + node.unalias().alias
            return sqlglot.parse_one(chain_added_table, read=DuneSQL)

        return node

    return table_replacement_transform
