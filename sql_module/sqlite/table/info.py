# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass, field
import datetime

# 主要要素
from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.name import TableName
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column import Column
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint


@dataclass
class Info:
    driver: Driver
    table_name: TableName

    def __post_init__(self):
        """必要な情報を取得"""
        self.driver.execute(f"PRAGMA table_info({self.table_name.now});")
        self.raw_column_list = self.driver.fetchall(True)
        self.driver.execute(f"PRAGMA foreign_key_list({self.table_name.now});")
        self.raw_foreign_key_list = self.driver.fetchall(True)
        self.driver.execute(f"PRAGMA index_list({self.table_name.now});")
        self.raw_index_list = self.driver.fetchall(True)
