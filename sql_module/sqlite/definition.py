from dataclasses import dataclass, field
from pathlib import Path
import datetime
from typing import Literal
from abc import abstractmethod

from sql_module import (
    Table,
    Column,
    CompositeConstraint,
    Query,
    Field,
    conds,
    Join,
    OrderBy,
    Insert,
    Update,
    Select,
    utils,
    expressions,
)


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

    def exists(self) -> bool:
        return self.table.exists()

    def create_index(
        self,
        column_list: list[Column] | Column,
        exists_ok: bool = True,
        is_unique: bool = False,
        where: conds.Cond | None = None,
        index_name: str | None = None,
        time_log: utils.LogLike | None = None,
    ):
        self.table.create_index(column_list, exists_ok, is_unique, where, index_name, time_log=time_log)

    def delete_index(
        self,
        column_list: list[Column] | Column,
        not_exists_ok: bool = True,
        is_unique: bool = False,
        index_name: str | None = None,
        time_log: utils.LogLike | None = None,
    ):
        self.table.delete_index(column_list, not_exists_ok, is_unique, index_name, time_log=time_log)

    def create(
        self,
        composite_constraint: list[CompositeConstraint] | CompositeConstraint | None = None,
        exists_ok: bool = True,
        is_execute: bool = True,
        time_log: utils.LogLike | None = None,
    ) -> Query:
        column_list = self._get_create_column()
        create = self.table.create(column_list, composite_constraint, exists_ok, is_execute, time_log=time_log)
        return create

    def insert(
        self,
        record: list[Field] | Field,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Insert:
        insert = self.table.insert(record, is_execute, is_returning_id, time_log=time_log)
        return insert

    def bulk_insert(self, insert_list: list[Insert], time_log: utils.LogLike | None = None):
        self.table.bulk_insert(insert_list, time_log=time_log)

    def bulk_insert2(self, insert_list: list[Insert], time_log: utils.LogLike | None = None):
        self.table.bulk_insert2(insert_list, time_log=time_log)

    def update(
        self,
        record: list[Field] | Field,
        where: conds.Cond | None = None,
        non_where_safe: bool = True,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Update:
        update = self.table.update(record, where, non_where_safe, is_execute, is_returning_id, time_log=time_log)
        return update

    def select(
        self,
        expression: list[expressions.Expression | Literal[1]] | expressions.Expression | Literal[1] | None = None,
        where: conds.Cond | None = None,
        join: list[Join] | Join | None = None,
        group_by: list[Column] | Column | None = None,
        order_by: list[OrderBy] | OrderBy | None = None,
        having: conds.Cond | None = None,
        limit: int | None = None,
        is_from: bool = True,
        is_execute: bool = True,
        time_log: utils.LogLike | None = None,
    ) -> Select:
        # もしかしたらサブクエリやるかも
        select = self.table.select(
            expression, where, join, group_by, order_by, having, limit, is_from, is_execute, time_log=time_log
        )
        return select

    def _get_create_column(self):
        attrs = self.__dict__.values()
        column_list = [attr for attr in attrs if isinstance(attr, Column)]
        return column_list


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

    def _get_append_update_column_record(self, record: list[Field] | Field) -> list[Field]:
        if isinstance(record, Field):
            record = [record]
        record = record.copy()
        # upsert・upsert時に必要
        update_field = Field(self.updated_at_column, datetime.datetime.now(datetime.timezone.utc))
        record.append(update_field)
        return record

    def insert(
        self,
        record: list[Field] | Field,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Insert:
        record = self._get_append_update_column_record(record)

        insert = self.table.insert(record, is_execute, is_returning_id, time_log=time_log)
        return insert

    # bulk_insert2はinsertにupdated_atを織り込み済みなのでオーバーライド不要
    def bulk_insert(
        self,
        record_list: list[list[Field]] | list[Field],
        time_log: utils.LogLike | None = None,
    ):
        record_list2 = []
        for record in record_list:
            record = self._get_append_update_column_record(record)
            record_list2.append(record)

        self.table.bulk_insert(record_list2, time_log=time_log)

    def update(
        self,
        record: list[Field] | Field,
        where: conds.Cond | None = None,
        non_where_safe: bool = True,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Update:
        record = self._get_append_update_column_record(record)

        insert = self.table.update(record, where, non_where_safe, is_execute, is_returning_id, time_log=time_log)
        return insert

    @abstractmethod
    def set_colmun_difinition(self):
        pass


class SQLiteMaster(TableDefinition):
    """
    目指せsqliteマスター!!
    """

    def set_colmun_difinition(self):
        self.type_column = self.get_column("type", str, not_null=True)
        self.name_column = self.get_column("name", str, not_null=True)
        self.tbl_name_column = self.get_column("name", str, not_null=True)
        self.rootpage_column = self.get_column("rootpage", int, unique=True, not_null=True)
        self.sql_column = self.get_column("sql", str)
