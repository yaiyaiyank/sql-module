from yai.entry import *
from yai.entry.sql_module import sqlite

from ..sql_value import python_value_to_sql_value


class Field:
    """insertや"""

    column: sqlite.Column
    value: str | int | bytes | Path | datetime.date | None
    upsert: bool = False
    is_log: bool = True

    @property
    def sql_value(self):
        return python_value_to_sql_value(self.value)
