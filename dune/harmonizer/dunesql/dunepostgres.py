from sqlglot.dialects.postgres import Postgres


class DunePostgres(Postgres):
    """
    Overwrite Postgres dialect to nulls are last
    """

    NULL_ORDERING = "nulls_are_last"
