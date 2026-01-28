from dataclasses import dataclass, field
import datetime
from abc import abstractmethod

from sql_module import Table, Column, CompositeConstraint, Query, Field, wheres


@dataclass
class TableDefinition:
    """
    self.name_columnみたいにカラムにアクセスできる、カラムンベースなクラス
    createやinsert対応
    """

    table: Table

    def __post_init__(self):
        self.set_colmun_difinition()

    @abstractmethod
    def set_colmun_difinition(self):
        """カラムの定義"""

    def info(self, show: bool = True):
        """テーブルの生の声"""
        return self.table.info(show)

    def create(
        self,
        composite_constraint: list[CompositeConstraint] | CompositeConstraint | None = None,
        exists_ok: bool = True,
        is_execute: bool = True,
    ) -> Query:
        column_list = self._get_create_column()
        create = self.table.create(column_list, composite_constraint, exists_ok, is_execute)
        return create

    def insert(self, record: list[Field], is_execute: bool = True):
        insert = self.table.insert(record, is_execute)
        return insert

    def _get_create_column(self):
        attrs = self.__dict__.values()
        column_list = [attr for attr in attrs if isinstance(attr, Column)]
        return column_list


@dataclass
class IDTableDefinition(TableDefinition):
    """
    self.id_columnやself.name_columnみたいに定義したカラムにアクセスできるクラス
    self.id_columnを標準搭載し、create_table時にself.id_columnが必ず先頭にくるようにテーブルのカラムが定義される
    createやinsert対応
    """

    def __post_init__(self):
        self._set_id_colmun()
        self.set_colmun_difinition()

    def _set_id_colmun(self):
        self.id_column = self.table.get_column("id", type=int, primary=True)

    @abstractmethod
    def set_colmun_difinition(self):
        pass


@dataclass
class AtIDTableDefinition(IDTableDefinition):
    """
    self.id_columnやself.name_columnみたいに定義したカラムにアクセスできるクラス
    self.id_columnとself.created_at_columnとself.updated_at_columnを標準搭載し、
    create_table時にself.id_columnが必ず先頭にきて、self.created_at_column, self.updated_at_columnの順に最後にテーブルのカラムが定義される
    createやinsert対応
    """

    def __post_init__(self):
        self._set_id_colmun()
        self.set_colmun_difinition()
        self._set_at_colmun()

    def _set_at_colmun(self):
        self.created_at_column = self.table.get_column(
            "created_at", type=datetime.datetime, default_value="CURRENT_TIMESTAMP"
        )
        self.updated_at_column = self.table.get_column(
            "updated_at", type=datetime.datetime, default_value="CURRENT_TIMESTAMP"
        )

    def insert(self, record: list[Field], is_execute: bool = True):
        # upsertでのupdate時に必要
        update_field = Field(self.updated_at_column, datetime.datetime.now(datetime.timezone.utc))
        record.append(update_field)

        insert = self.table.insert(record, is_execute)
        return insert

    def update(self, record: list[Field], where: wheres.Where | None = None, is_execute: bool = True) -> Query:
        # updateで必要
        update_field = Field(self.updated_at_column, datetime.datetime.now(datetime.timezone.utc))
        record.append(update_field)

        insert = self.table.update(record, where, is_execute)
        return insert

    def select(
        self, column_list: list[Column] | None = None, where: wheres.Where | None = None, is_execute: bool = True
    ) -> Query:
        # もしかしたらサブクエリやるかも
        select = self.table.select(column_list, where, is_execute)
        return select

    @abstractmethod
    def set_colmun_difinition(self):
        pass
