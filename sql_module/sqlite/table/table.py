# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass, field
import datetime
from typing import Self, Literal

# 主要要素
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint

# info
from sql_module.sqlite.table.info import Info

# create系
from sql_module import CompositeConstraint
from sql_module.sqlite.table.create.query_builder import CreateQueryBuilder

# insert系
from sql_module.sqlite.table.insert.query_builder import InsertQueryBuilder, Insert

# update系
from sql_module.sqlite.table.update.query_builder import UpdateQueryBuilder, Update

# select系
from sql_module.sqlite.table.select.query_builder import SelectQueryBuilder, Select

# utils
from sql_module import (
    exceptions,
    utils,
    Driver,
    Query,
    query_join_comma,
    Column,
    Field,
    conds,
    Join,
    OrderBy,
    expressions,
)

# テーブルのメイン操作系


@dataclass
class Table:
    """
    テーブルを司るクラス。TableDefinitonフレームワークに役割を奪われつつあり、pathlibに追われたosライブラリみたいな感じ。
    """

    driver: Driver
    name: str

    def __repr__(self) -> str:
        text = f"テーブル名: {self.name}"
        return text

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
        """カラムを取得"""
        # カラム名
        column_name = ColumnName(name, self.name)
        # 列制約
        column_constraint = ColumnConstraint(
            python_type=type,
            unique=unique,
            not_null=not_null,
            primary=primary,
            references=references,
            default_value=default_value,
        )
        # カラム
        column = Column(driver=self.driver, name=column_name, constraint=column_constraint)
        return column

    def exists(self) -> bool:
        """
        存在しているかどうか
        # placeholder = {"p0": "table", "p1": "post"}
        # select.driver.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = :p0 AND name = :p1)", placeholder)
        """
        table = Table(driver=self.driver, name="sqlite_master")
        type_column = table.get_column("type", str)
        name_column = table.get_column("name", str)
        sqlite_master_select = table.select(
            1, conds.Eq(type_column, "table") & conds.Eq(name_column, self.name), is_execute=False
        )
        select = self.select(conds.Exists(sqlite_master_select), is_from=False)
        result = select.fetchone()[0]
        if result:
            return True
        else:
            return False

    def info(self, show: bool = True) -> Info:
        """
        このテーブルの持つ情報を詰め込んだinfoオブジェクトを取得。infoオブジェクトは以下の変数を持つ
        - column_info_list: 各カラムの情報(複合除く)
        - multi_index_list: 複合ユニークや複合インデックス情報
        - raw_column_list: PRAGMA table_info(テーブル名);をfetchしたもの
        - raw_foreign_key_list: PRAGMA foreign_key_list(テーブル名);をfetchしたもの
        - raw_index_list: PRAGMA index_list(テーブル名);をfetchしたもの
        """
        if not self.exists():
            return f"テーブル: {self.name} が作成されていません。"
        info = Info(self.driver, self.name)
        info.set_info()
        if show:
            print(info)
        return info

    def make_index(self, column_list: list[Column] | Column, exists_ok: bool = True):
        """インデックス生成(複合)"""
        if isinstance(column_list, Column):
            column_list.make_index(exists_ok)
            return
        if isinstance(column_list, list):
            columns_query = utils.join_comma([column.name.name for column in column_list])
            columns_name = utils.join_under(sorted([column.name.name for column in column_list]))
            if exists_ok:
                self.driver.execute(
                    f"CREATE INDEX IF NOT EXISTS idx_{self.name}_{columns_name} ON {self.name}({columns_query})"
                )
            else:
                self.driver.execute(f"CREATE INDEX idx_{self.name}_{columns_name} ON {self.name}({columns_query})")
            return
        raise TypeError("index作成はカラムのみです。")

    def delete_index(self, column_list: list[Column] | Column, not_exists_ok: bool = True):
        """インデックス削除(複合)"""
        if isinstance(column_list, Column):
            column_list.delete_index(not_exists_ok)
            return
        if isinstance(column_list, list):
            columns_name = utils.join_under(sorted([column.name.name for column in column_list]))
            if not_exists_ok:
                self.driver.execute(f"DROP INDEX IF EXISTS idx_{self.name}_{columns_name}")
            else:
                self.driver.execute(f"DROP INDEX idx_{self.name}_{columns_name}")
            return
        raise TypeError("index削除はカラムのみです。")

    def create(
        self,
        column_list: list[Column],
        composite_constraint: list[CompositeConstraint] | CompositeConstraint | None = None,
        exists_ok: bool = True,
        is_execute: bool = True,
    ) -> Query:
        """
        テーブル作成

        クエリ例:
        'CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)'

        Args:
            column_list (list[Column]): テーブルの各カラムの型の設定
            composite_constraint_list (list[CompositeConstraint] | None): 複合キー制約
            exists_ok (bool): 既にテーブルがあってもok。既に作られたテーブルを上書くことはない
            is_execute (bool): 実行するかどうか。戻り値のCreateオブジェクトをexecuteするならFalseにしないと2度実行されてしまうので注意
        """
        query_builder = CreateQueryBuilder(self.driver)
        # 最初のクエリ
        head_query = query_builder.get_head_query(exists_ok)
        # 列定義・列制約のクエリ
        column_define_constraint_query = query_builder.get_column_define_constraint_query(column_list)
        # 表制約のクエリ
        composite_constraint_query = query_builder.get_composite_constraint_query(composite_constraint)
        # 制約クエリ(列+表)
        constraint_query = query_join_comma([column_define_constraint_query, composite_constraint_query], no_empty=True)

        create = head_query + f" {self.name} (" + constraint_query + ")"

        if is_execute:
            create.execute()
            create.commit()

        return create

    def insert(
        self,
        record: list[Field] | Field,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Insert:
        """
        行を挿入

        クエリ・パラメータ例:
        'INSERT INTO users (name, age) VALUES (:p0, :p1)'
        {'p0': 'Alice', 'p1': 30}

        クエリ・パラメータ例2:
        'INSERT INTO work (site_id, content_id, title, channel_id) VALUES (:p0, :p1, :p2, :p3) ON CONFLICT (site_id, content_id) DO UPDATE SET title = excluded.title, channel_id = excluded.channel_id'
        {'p0': 3, 'p1': 20, 'p2': 'おお', 'p3': 1}
        """
        query_builder = InsertQueryBuilder(self.driver)
        # 最初のクエリ
        head_query = query_builder.get_head_query()
        # VALUES
        value_query = query_builder.get_value_query(record)
        # ON CONFLICT
        on_conflict_query = query_builder.get_on_conflict_query(record)
        # RETURNING id
        returning_id_query = query_builder.get_returning_id_query(is_returning_id)

        insert_base = head_query + f" {self.name} " + value_query + " " + on_conflict_query + " " + returning_id_query

        insert = Insert()
        insert.straight_set(insert_base)

        if is_execute:
            insert.execute(time_log=time_log)
            # fetch待ちがあると 「OperationalError: cannot commit transaction - SQL statements in progress」になるので、fetch_idしないときのみコミット。
            if not is_returning_id:
                insert.commit(time_log=time_log)

        return insert

    def bulk_insert(self, insert_list: list[Insert], time_log: Literal["print_log"] | utils.PrintLog | None = None):
        """
        バルクインサートをついに実装！

        Args:
            insert_list (list[Insert]): まだ実行していないInsertオブジェクトのリスト
        """
        timer = utils.Timer(time_log=time_log)

        self.driver.begin()
        try:
            for i, insert in enumerate(insert_list):
                insert.execute()
        except Exception as e:
            self.driver.rollback()
            raise RuntimeError(f"バルクindex={i}, query(placeholderじゃない部分)={insert} failed: {e}") from e

        self.driver.commit()

        timer.finish("バルクinsert時間")

    def update(
        self,
        record: list[Field] | Field,
        where: conds.Cond | None = None,
        non_where_safe: bool = True,
        is_execute: bool = True,
        is_returning_id: bool = False,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Update:
        """
        行を更新

        クエリ・パラメータ例:
        'UPDATE users SET name = :p0, age = :p1 WHERE id = :p2'
        {'p0': 'Alice', 'p1': 30, 'p2': 1}
        """
        if non_where_safe and where is None:
            raise exceptions.DefenseAccidentException(
                "where無しでupdateする場合、事故防止のためにnon_where_safe引数をFalseにしてください。"
            )
        query_builder = UpdateQueryBuilder(self.driver)
        # 最初のクエリ
        head_query = query_builder.get_head_query()
        # set部分
        set_query = query_builder.get_set_query(record)
        # where部分
        where_query = query_builder.get_where_query(where)
        # RETURNING id
        returning_id_query = query_builder.get_returning_id_query(is_returning_id)

        update_base = head_query + f" {self.name} " + set_query + " " + where_query + " " + returning_id_query

        update = Update()
        update.straight_set(update_base)

        if is_execute:
            update.execute(time_log=time_log)
            update.commit(time_log=time_log)

        return update

    def select(
        self,
        expression: list[expressions.Expression | Literal[1]] | expressions.Expression | Literal[1] | None = None,
        where: conds.Cond | None = None,
        join: list[Join] | Join | None = None,
        group_by: list[Column] | Column | None = None,
        order_by: list[OrderBy] | OrderBy | None = None,
        having: None = None,
        limit: int | None = None,
        is_from: bool = True,
        is_execute: bool = True,
        time_log: Literal["print_log"] | utils.PrintLog | None = None,
    ) -> Select:
        """
        行を更新

        クエリ・パラメータ例:
        'SELECT * FROM work WHERE id = :p0'
        {'p0': 3}
        """
        query_builder = SelectQueryBuilder(self.driver)
        # 最初のクエリ
        head_query = query_builder.get_head_query()
        # expression部分のクエリ
        expression_query = query_builder.get_expression_query(expression)
        # from部分のクエリ
        from_query = query_builder.get_from_query(is_from, self.name)
        # join部分のクエリ
        join_query = query_builder.get_join_query(join)
        # where部分のクエリ
        where_query = query_builder.get_where_query(where)
        # group_by部分のクエリ
        group_by_query = query_builder.get_group_by_query(group_by)
        # having部分のクエリ
        having_query = query_builder.get_having_query(having)
        # order_by部分のクエリ
        order_by_query = query_builder.get_order_by_query(order_by)
        # limit部分のクエリ
        limit_query = query_builder.get_limit_query(limit)

        select_base = (
            head_query
            + " "
            + expression_query
            + " "
            + from_query
            + " "
            + join_query
            + " "
            + where_query
            + " "
            + group_by_query
            + " "
            + having_query
            + " "
            + order_by_query
            + " "
            + limit_query
        )

        select = Select()

        select.set_select_type(query_builder.select_type)

        select.straight_set(select_base)

        if is_execute:
            select.execute(time_log=time_log)

        return select
