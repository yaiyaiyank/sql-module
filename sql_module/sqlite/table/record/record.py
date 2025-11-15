# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime

# このライブラリ
from sql_module import Column
from sql_module.sqlite.table.sql_value import python_value_to_sql_value


@dataclass
class Field:
    """insertや"""

    column: Column
    value: str | int | bytes | Path | datetime.date | None
    upsert: bool = False
    is_log: bool = True

    @property
    def sql_value(self):
        return python_value_to_sql_value(self.value)
