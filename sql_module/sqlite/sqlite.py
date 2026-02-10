from pathlib import Path
from dataclasses import dataclass

from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.name import TableName

from sql_module import Table, TableDefinition, IDTableDefinition, AtIDTableDefinition, Query

# utils
from sql_module import utils


@dataclass
class SQLiteDataBase:
    db_path: Path | str | None = None
    is_wal_mode: bool = True

    def __post_init__(self):
        if self.db_path is None:
            self.db_path = ":memory:"
        self.driver = Driver(self.db_path)
        self._set_journal_mode()

    def get_table(self, name: str) -> Table:
        table_name = TableName(name)
        return Table(driver=self.driver, name=table_name)

    def get_table_definition(self, table_definition_class: type, name: str | None = None) -> TableDefinition:
        """
        TableDefinitionオブジェクトを取得
        デフォルトではtable_definition_classのキャメルをスネークしたものをテーブル名とする
        """
        if name is None:
            name = utils.camel_to_snake(table_definition_class.__name__)
        table = self.get_table(name)

        table_definition = table_definition_class(table)
        return table_definition

    def get_exists_table_list(self) -> list[Table]:
        self.driver.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.driver.fetchall()
        return [self.get_table(table[0]) for table in tables]

    def _set_journal_mode(self):
        self.driver.execute("PRAGMA journal_mode")
        journal_mode = self.driver.fetchone()["journal_mode"]
        if journal_mode == "delete" and self.is_wal_mode:
            self.driver.execute("PRAGMA journal_mode = WAL")
        if journal_mode == "wal" and not self.is_wal_mode:
            self.driver.execute("PRAGMA journal_mode = DELETE")
