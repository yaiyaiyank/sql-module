from yai.entry import Log, dataclass, field, Path, datetime, Literal, base_utils_module

# 主要要素
from ..driver import Driver
from .name import TableName
from .column.name import ColumnName
from yai.entry.sql_module.sqlite import Column
from yai.entry.sql_module.sqlite import Field
from .column.column import ColumnConstraint

# create系
from yai.entry.sql_module.sqlite.constraint import CompositeConstraint
from yai.entry.sql_module.sqlite.querables import Create
from .create.query_builder import CreateQueryBuilder

# insert系
from yai.entry.sql_module.sqlite.querables import Insert
from .insert.query_builder import InsertQueryBuilder

# exceptions
from ...exceptions import ColumnAlreadyRegistrationError

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
    log: Log = field(default_factory=Log)

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

    def make_index(self, column_list: list[Column]):
        """インデックス生成(複合)"""

    def create(
        self,
        column_list: list[Column],
        composite_constraint_list: list[CompositeConstraint] | None = None,
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
        composite_constraint_query = query_builder.get_composite_constraint_query(composite_constraint_list)
        # 制約クエリ(列+表)
        constraint_query = base_utils_module.str_.join_comma(
            [column_define_constraint_query, composite_constraint_query], no_empty=True
        )

        query = f"{head_query} {self.name.now} {constraint_query}"
        create = Create(driver=self.driver, query=query)

        if is_execute:
            create.execute()

        return create

    def insert(self, record: list[Field]):
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
