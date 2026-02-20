# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime

# このライブラリ
from sql_module import Column, conds


@dataclass
class Field:
    """insertやupdateなどで挿入するレコードの要素"""

    column: Column
    value: str | int | bytes | Path | datetime.date | None
    upsert: bool = False

    @property
    def sql_value(self):
        return self.column.constraint.get_sql_value(self.value)

    def __repr__(self) -> str:
        string = f"カラム名: {self.column.name}\nvalue: {self.value}\nupsert: {self.upsert}"
        return string

    def to_eq(self, is_column_only_name: bool = False) -> conds.Eq:
        eq = conds.Eq(self.column, self.value, is_column_only_name)
        return eq
