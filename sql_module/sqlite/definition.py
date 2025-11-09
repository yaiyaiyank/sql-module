from yai.entry import dataclass, abstractmethod, datetime
from yai.entry.sql_module import sqlite


@dataclass
class TableDefinition:
    """
    self.id_columnみたいにカラムにアクセスできるクラス
    self.id_columnを標準搭載し、create_table時にself.id_columnが必ず先頭にくる
    """

    table: sqlite.Table

    def __post_init__(self):
        self.set_colmun_difinition()

    @abstractmethod
    def set_colmun_difinition(self):
        """カラムの定義"""

    def create(
        self,
        composite_constraint_list: list[sqlite.constraint.CompositeConstraint] | None = None,
        is_column_sort: bool = True,
    ):
        column_list = self._get_create_column(is_column_sort)
        self.table.create(column_list, composite_constraint_list)

    def _get_create_column(self, is_column_sort: bool):
        attrs = self.__dict__.values()
        column_list = [attr for attr in attrs if isinstance(attr, sqlite.Column)]
        return column_list


@dataclass
class IDTableDefinition(TableDefinition):
    def __post_init__(self):
        self.id_column = self.table.get_column("id", type=int, primary=True)
        self.set_colmun_difinition()

    @abstractmethod
    def set_colmun_difinition(self):
        pass


@dataclass
class AtDateIDTableDefinition(TableDefinition):
    def __post_init__(self):
        self.id_column = self.table.get_column("id", type=int, primary=True)
        self.set_colmun_difinition()
        self.created_at_column = self.table.get_column(
            "created_at", type=datetime.datetime, default_value="CURRENT_TIMESTAMP"
        )
        self.updated_at_column = self.table.get_column(
            "updated_at", type=datetime.datetime, default_value="CURRENT_TIMESTAMP"
        )

    @abstractmethod
    def set_colmun_difinition(self):
        pass
