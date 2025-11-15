from dataclasses import dataclass
from sql_module.sqlite.table.column.interface import ColumnLike
from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint


@dataclass
class Column(ColumnLike):  # ColumnLikeは別ファイルのColumnConstraintと相互依存しているため
    """
               ↓ ここ
    Table -> Column -> Field
    """

    driver: Driver
    name: ColumnName
    constraint: ColumnConstraint

    def make_index(self):
        """インデックス生成"""
