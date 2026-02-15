from dataclasses import dataclass
from sql_module.sqlite.table.column.interface import ColumnLike
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint

from sql_module import Driver, expressions


@dataclass
class Column(ColumnLike, expressions.Expression):  # ColumnLikeは別ファイルのColumnConstraintと相互依存しているため
    """
    スシカラム
    """

    driver: Driver
    name: ColumnName
    constraint: ColumnConstraint

    def make_index(self, exists_ok: bool = True):
        """インデックス生成"""
        if exists_ok:
            self.driver.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.name.table_name}_{self.name.name} ON {self.name.table_name}({self.name.name})"
            )
        else:
            self.driver.execute(
                f"CREATE INDEX idx_{self.name.table_name}_{self.name.name} ON {self.name.table_name}({self.name.name})"
            )

    def delete_index(self, not_exists_ok: bool = True):
        """インデックス削除"""
        if not_exists_ok:
            self.driver.execute(f"DROP INDEX IF EXISTS idx_{self.name.table_name}_{self.name.name}")
        else:
            self.driver.execute(f"DROP INDEX idx_{self.name.table_name}_{self.name.name}")
