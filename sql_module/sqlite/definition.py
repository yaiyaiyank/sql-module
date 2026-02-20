from dataclasses import dataclass, field
from pathlib import Path
import datetime
from typing import Literal
from abc import abstractmethod
import sqlite3

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

    def info(self, show: bool = False):
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
        is_commit: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Insert:
        insert = self.table.insert(record, is_execute, is_commit, is_returning_id, time_log=time_log)
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
        is_commit: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Update:
        update = self.table.update(
            record, where, non_where_safe, is_execute, is_commit, is_returning_id, time_log=time_log
        )
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

    def _get_append_update_column_record(
        self, record: list[Field] | Field, now: datetime.datetime | None = None
    ) -> list[Field]:
        if isinstance(record, Field):
            record = [record]
        record = record.copy()
        # upsert・upsert時に必要
        if now is None:
            now = datetime.datetime.now(datetime.timezone.utc)
        update_field = Field(self.updated_at_column, now)
        record.append(update_field)
        return record

    def insert(
        self,
        record: list[Field] | Field,
        is_execute: bool = True,
        is_commit: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Insert:
        record = self._get_append_update_column_record(record)

        insert = self.table.insert(record, is_execute, is_commit, is_returning_id, time_log=time_log)
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
        is_commit: bool = True,
        is_returning_id: bool = False,
        time_log: utils.LogLike | None = None,
    ) -> Update:
        record = self._get_append_update_column_record(record)

        update = self.table.update(
            record, where, non_where_safe, is_execute, is_commit, is_returning_id, time_log=time_log
        )
        return update

    @abstractmethod
    def set_colmun_difinition(self):
        pass


class SCD2AtIDTableDefinition(AtIDTableDefinition):
    """
    self.id_columnやself.name_columnみたいに定義したカラムにアクセスできるクラス
    self.id_columnとself.created_at_columnとself.updated_at_columnとself.valid_from_columnとself.valid_to_columnとself.is_current_columnを標準搭載し、
    create_table時にself.id_columnが必ず先頭にきて、self.valid_from_column, self.valid_to_column, self.is_current_column, self.created_at_column, self.updated_at_columnの順に最後にテーブルのカラムが定義される
    createやinsert対応
    """

    def __post_init__(self):
        self._set_id_colmun()
        self.set_colmun_difinition()
        self._set_scd_colmun()
        self._set_at_colmun()

    def _set_scd_colmun(self):
        # SCD2で期間の始まりを示すカラム。insertで新規に入った際につける。
        self.valid_from_column = self.get_column("valid_from", datetime.datetime, not_null=True)
        # SCD2で期間の終わりを示すカラム。updateで新規と入れ替わる際につける。
        self.valid_to_column = self.get_column("valid_to", datetime.datetime)
        # 最新
        self.is_current_column = self.get_column("is_current", bool, not_null=True, default_value=True)

    def create_is_current_unique_index(
        self,
        column_list: list[Column] | Column,
        is_current_index_name: str | None = None,
        is_valid_index: bool = True,
        is_valid_index_name: str | None = None,
    ):
        """
        「同一のユニークで current は1行だけ」を保証
        ついでに時点参照を速くできる

        ChatGPTの意見より実装

        Args:
            column_list (list[Column] | Column): 履歴なしで本来ユニークキーなカラム
            is_valid_index (bool): 時点参照を速くできる
        """
        if isinstance(column_list, Column):
            column_list = [column_list]

        # 「同一のユニークで current は1行だけ」を保証
        where = conds.Eq(self.is_current_column, True, True)
        self.create_index(column_list, is_unique=True, where=where, index_name=is_current_index_name)

        # ついでに時点参照を速くできるするですよ
        if not is_valid_index:
            return

        index_column_list = column_list.copy() + [self.valid_from_column, self.valid_to_column]
        self.create_index(index_column_list, index_name=is_valid_index_name)

    def regist_current(
        self,
        update_unique_where: conds.Cond,
        insert_record: list[Field] | Field,
        time_log: utils.LogLike | None = None,
    ) -> int:
        """
        Args:
            update_unique_where: 履歴なしで本来ユニークキーなカラムたちのeq
            insert_record: insertするレコード

        Returns:
            current_id (int): insertしたcurrent=1の行のid
        """
        # トランザクションで一括
        self.table.driver.begin()

        now = datetime.datetime.now(datetime.timezone.utc)

        # まずは現行を閉じる
        user_update_record = [Field(self.valid_to_column, now), Field(self.is_current_column, False)]
        user_update_record = self._get_append_update_column_record(user_update_record)

        where = update_unique_where & conds.Eq(self.is_current_column, True)

        self.table.update(user_update_record, where, is_commit=False, time_log=time_log)
        # 新版を追加
        # + SCD系カラム
        insert_record2 = insert_record.copy() + [
            Field(self.valid_from_column, now),
            Field(self.is_current_column, True),
        ]
        # + updated_at系カラム
        insert_record3 = self._get_append_update_column_record(insert_record2, now=now)
        # まだコミットしない(id拾うため)
        insert = self.table.insert(insert_record3, is_returning_id=True, time_log=time_log)
        fetch_id = insert.fetch_id(time_log=time_log)
        # ここでコミット
        insert.commit(time_log=time_log)

        return fetch_id

    def select_current(
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
        """whereにis_current_column=1の条件を追加してselect"""
        if where is None:
            where = conds.TRUE()

        where = where & conds.Eq(self.is_current_column, True)

        select = self.table.select(
            expression, where, join, group_by, order_by, having, limit, is_from, is_execute, time_log=time_log
        )
        return select

    def history(
        self,
        start_date: datetime.date | str | None = None,
        end_date: datetime.date | str | None = None,
        expression: list[expressions.Expression | Literal[1]] | expressions.Expression | Literal[1] | None = None,
        where: conds.Cond | None = None,
        is_asc: bool = False,
        dict_output: bool = False,
    ) -> list[dict[str]] | list[sqlite3.Row]:
        """履歴。valid_fromが半開区間[start_date, end_date)の間で指定可能"""
        if where is None:
            where = conds.TRUE()

        where = where & conds.Range(self.valid_from_column, start_date, end_date)

        order_by = [OrderBy(self.valid_from_column, is_asc), OrderBy(self.id_column, is_asc)]

        select = self.select(expression, where, order_by=order_by)
        return select.fetchall(dict_output)

    def point(
        self,
        date: datetime.date | str,
        expression: list[expressions.Expression | Literal[1]] | expressions.Expression | Literal[1] | None = None,
        where: conds.Cond | None = None,
        dict_output: bool = False,
    ) -> list[dict[str]] | list[sqlite3.Row]:
        """時点。dateが半開区間[valid_from, valid_to or None)の間で指定可能"""
        if where is None:
            where = conds.TRUE()

        # 開始日
        where = where & conds.LessEq(self.valid_from_column, date)
        # 終了日
        where = where & (conds.Greater(self.valid_to_column, date) | conds.Eq(self.valid_to_column, None))

        select = self.select(expression, where)
        return select.fetchall(dict_output)


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

    def get_exists_table_name_list(self) -> list[str]:
        select = self.select(self.name_column, conds.Eq(self.type_column, "table"))
        tables = select.fetchall()  # (table_name1,), (table_name2,), ...,(table_namen,) みたいな値になる
        return [table[0] for table in tables]
