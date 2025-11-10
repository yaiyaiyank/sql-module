from pathlib import Path
from dataclasses import dataclass

from sql_module.sqlite.definition import TableDefinition, AtDateIDTableDefinition
from sql_module.sqlite.table.table import Table
from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.name import TableName


@dataclass
class SQLiteDataBase:
    db_path: Path | str | None = None

    def __post_init__(self):
        if self.db_path is None:
            self.db_path = ":memory:"
        self.driver = Driver(self.db_path)

    def get_table(self, name: str) -> Table:
        table_name = TableName(name)
        return Table(driver=self.driver, name=table_name)

    def get_table_list(self) -> list[Table]:
        self.driver.execute_cursor("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.driver.fetchall()
        return [Table(driver=self.driver, name=TableName(table[0])) for table in tables]
