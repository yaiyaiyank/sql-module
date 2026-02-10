from dataclasses import dataclass, field
from pathlib import Path
import datetime
from typing import Literal
from abc import abstractmethod

from sql_module import Table, Column, CompositeConstraint, Query, Field, wheres, Insert, Update, Select, utils


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

    def get_column(
        self,
        name: str,
        type: type,
        unique: bool = False,
        not_null: bool = False,
        primary: bool = False,  # AUTO_INCREMENTは廃止されました。そのうちuuid対応するかも
        references: Column | None = None,
        default_value: str | int | bytes | Path | datetime.date | None = None,  # bool, datetime.datetime内包
    ) -> Column:
        return self.table.get_column(name, type, unique, not_null, primary, references, default_value)

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

    def insert(
        self,
        record: list[Field] | Field,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Insert:
        insert = self.table.insert(record, is_execute, is_returning_id, time_log=time_log)
        return insert

    def bulk_insert(self, insert_list: list[Insert], time_log: Literal["print_log"] | utils.PrintLog | None = None):
        self.table.bulk_insert(insert_list, time_log=time_log)

    def update(
        self,
        record: list[Field] | Field,
        where: wheres.Where | None = None,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Update:
        update = self.table.update(record, where, is_execute, is_returning_id, time_log=time_log)
        return update

    def select(
        self,
        column_list: list[Column] | Column | None = None,
        where: wheres.Where | None = None,
        is_execute: bool = True,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Select:
        # もしかしたらサブクエリやるかも
        select = self.table.select(column_list, where, is_execute, time_log=time_log)
        return select

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

    def insert(
        self,
        record: list[Field] | Field,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Insert:
        if isinstance(record, Field):
            record = [record]
        record = record.copy()
        # upsertでのupdate時に必要
        update_field = Field(self.updated_at_column, datetime.datetime.now(datetime.timezone.utc))
        record.append(update_field)

        insert = self.table.insert(record, is_execute, is_returning_id, time_log=time_log)
        return insert

    def update(
        self,
        record: list[Field] | Field,
        where: wheres.Where | None = None,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Update:
        if isinstance(record, Field):
            record = [record]
        record = record.copy()
        # updateで必要
        update_field = Field(self.updated_at_column, datetime.datetime.now(datetime.timezone.utc))
        record.append(update_field)

        insert = self.table.update(record, where, is_execute, is_returning_id, time_log=time_log)
        return insert

    @abstractmethod
    def set_colmun_difinition(self):
        pass
