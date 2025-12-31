from pathlib import Path
from dataclasses import dataclass

from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.name import TableName

from sql_module import Table, TableDefinition, IDTableDefinition, AtIDTableDefinition


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

    def get_table_definition(self, name: str, table_definition_class: TableDefinition) -> TableDefinition:
        """ここにWorkなどを渡す"""
        table = self.get_table(name)

        table_definition = table_definition_class(table)
        return table_definition

    def get_exists_table_list(self) -> list[Table]:
        self.driver.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.driver.fetchall()
        return [self.get_table(table[0]) for table in tables]
