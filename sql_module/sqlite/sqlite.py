from pathlib import Path
from dataclasses import dataclass

from sql_module.sqlite.driver import Driver

from sql_module import utils, Table, TableDefinition, IDTableDefinition, AtIDTableDefinition, SQLiteMaster, conds


@dataclass
class SQLiteDataBase:
    db_path: Path | str | None = None
    is_wal_mode: bool = True

    def __post_init__(self):
        self.driver = Driver(self.db_path)
        self._set_journal_mode()

    def get_table(self, name: str) -> Table:
        return Table(driver=self.driver, name=name)

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

    def get_sqlite_master_table_definition(self) -> SQLiteMaster:
        sqlite_master = self.get_table_definition(
            SQLiteMaster, "sqlite_master"
        )  # デフォルトで'sq_lite_master'となってしまうので指定
        return sqlite_master

    def get_exists_table_list(self) -> list[Table]:
        # SELECT name FROM sqlite_master WHERE type='table'
        sqlite_master = self.get_sqlite_master_table_definition()
        exists_table_name_list = sqlite_master.get_exists_table_name_list()
        return [self.get_table(exists_table_name) for exists_table_name in exists_table_name_list]

    def _set_journal_mode(self):
        self.driver.execute("PRAGMA journal_mode")
        journal_mode = self.driver.fetchone()["journal_mode"]
        if journal_mode == "delete" and self.is_wal_mode:
            self.driver.execute("PRAGMA journal_mode = WAL")
        if journal_mode == "wal" and not self.is_wal_mode:
            self.driver.execute("PRAGMA journal_mode = DELETE")
