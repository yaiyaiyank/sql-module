# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime

# このライブラリ
from sql_module import Column


@dataclass
class Field:
    """insertやupdateなどで挿入するレコードの要素"""

    column: Column
    value: str | int | bytes | Path | datetime.date | None
    upsert: bool = False

    @property
    def sql_value(self):
        return self.column.constraint.get_sql_value(self.value)
