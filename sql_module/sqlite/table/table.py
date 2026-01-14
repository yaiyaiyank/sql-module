# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass, field
import datetime

# 主要要素
from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.name import TableName
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column import Column
from sql_module.sqlite.table.record.record import Field
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint

# info
from sql_module.sqlite.table.info import Info

# create系
from sql_module import CompositeConstraint
from sql_module import Create
from sql_module.sqlite.table.create.query_builder import CreateQueryBuilder

# insert系
from sql_module import Insert
from sql_module.sqlite.table.insert.query_builder import InsertQueryBuilder

# update系
from sql_module import Update
from sql_module.sqlite.table.update.query_builder import UpdateQueryBuilder

# utils
from sql_module import utils

# exceptions
from sql_module.exceptions import ColumnAlreadyRegistrationError, FetchNotFoundError

# テーブルのメイン操作系


# TODO 複合カラムの設計変えるかも
# columnがTableの情報を使わずに行けるかも


@dataclass
class Table:
    """
        ↓ ここ
    Table -> Column -> Field
    """

    driver: Driver
    name: TableName

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
        column_name = ColumnName(self.name, name)
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
        self.driver.execute(f"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{self.name.now}' LIMIT 1")
        try:
            self.driver.fetchone()
            return True
        except FetchNotFoundError:
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
            return f"テーブル: {self.name.now} が作成されていません。"
        info = Info(self.driver, self.name)
        info.set_info()
        if show:
            print(info)
        return info

    def make_index(self, column_list: list[Column] | Column):
        """インデックス生成(複合)"""
        if isinstance(column_list, Column):
            column_list.make_index()
            return
        if isinstance(column_list, list):
            columns_query = utils.join_comma([column.name.now for column in column_list])
            columns_name = utils.join_under([column.name.now for column in column_list])
            self.driver.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{self.name.now}_{columns_name} ON {self.name.now}({columns_query});"
            )
            return
        raise TypeError("index作成はカラムのみです。")

    def create(
        self,
        column_list: list[Column],
        composite_constraint: list[CompositeConstraint] | CompositeConstraint | None = None,
        exists_ok: bool = True,
        is_execute: bool = True,
    ):
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
        query_builder = CreateQueryBuilder()
        # 最初のクエリ
        head_query = query_builder.get_head_query(exists_ok)
        # 列定義・列制約のクエリ
        column_define_constraint_query = query_builder.get_column_define_constraint_query(column_list)
        # 表制約のクエリ
        composite_constraint_query = query_builder.get_composite_constraint_query(composite_constraint)
        # 制約クエリ(列+表)
        constraint_query = utils.join_comma([column_define_constraint_query, composite_constraint_query], no_empty=True)

        query = f"{head_query} {self.name.now} ({constraint_query})"
        create = Create(driver=self.driver, query=query)

        if is_execute:
            create.execute()

        return create

    def insert(self, record: list[Field], is_execute: bool = True):
        """
        行を挿入
        今はバルク非対応

        クエリ・パラメータ例:
        'INSERT INTO users (name, age) VALUES (:p0, :p1)'
        {'p0': 'Alice', 'p1': 30}

        クエリ・パラメータ例2:
        'INSERT INTO work (site_id, content_id, title, channel_id) VALUES (:p0, :p1, :p2, :p3) ON CONFLICT (site_id, content_id) DO UPDATE SET title = excluded.title, channel_id = excluded.channel_id'
        {'p0': 3, 'p1': 20, 'p2': 'おお', 'p3': 1}
        """
        query_builder = InsertQueryBuilder()
        # 最初のクエリ
        head_query = query_builder.get_head_query()
        # VALUES
        value_query = query_builder.get_value_query(record)
        # ON CONFLICT
        on_conflict_query = query_builder.get_on_conflict_query(record)

        query = f"{head_query} {self.name.now} {value_query} {on_conflict_query}"
        insert = Insert(driver=self.driver, query=query)

        if is_execute:
            insert.execute()

        return insert

    def update(self, record: list[Field], where_record: list[Field] | None = None, is_execute: bool = True):
        """
        行を更新

        クエリ・パラメータ例:
        'UPDATE users SET name = :p0, age = :p1 WHERE id = :p2'
        {'p0': 'Alice', 'p1': 30, 'p2': 1}
        """
        query_builder = UpdateQueryBuilder()
        head_query = query_builder.get_head_query()
        set_query = query_builder.get_set_query(record)
        where_query = query_builder.get_where_query(where_record)

        query = utils.join_space([head_query, self.name.now, "SET", set_query, where_query], no_empty=True)
        update = Update(driver=self.driver, query=query, placeholder_dict=query_builder.placeholder_dict)

        if is_execute:
            update.execute()

        return update
