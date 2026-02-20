from dataclasses import dataclass
from typing import Self, Literal

from sql_module.sqlite.table.column.interface import ColumnLike
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint

from sql_module import Driver, utils, expressions, conds

# index系
from sql_module.sqlite.table.index.query_builder import IndexQueryBuilder


@dataclass
class Column(ColumnLike, expressions.Expression):  # ColumnLikeは別ファイルのColumnConstraintと相互依存しているため
    """
    スシカラム
    """

    driver: Driver
    name: ColumnName
    constraint: ColumnConstraint

    def create_index(
        self,
        exists_ok: bool = True,
        is_unique: bool = False,
        where: conds.Cond | None = None,
        index_name: str | None = None,
        time_log: utils.LogLike | None = None,
    ):
        """
        インデックス生成
        whereを使う場合、全てのカラム名はis_column_only_name=Trueで構成されている必要がある
        """
        column_name_list = [self.name]

        query_builder = IndexQueryBuilder(self.driver)
        head_query = query_builder.get_create_head_query(exists_ok, is_unique)
        _index_name = query_builder.get_index_name(column_name_list, index_name, is_unique)
        on_query = query_builder.get_on_query(column_name_list)
        where_query = query_builder.get_where_query(where)

        create_index = head_query + " " + _index_name + " " + on_query + " " + where_query

        create_index.execute(time_log=time_log)
        create_index.commit(time_log=time_log)

    def delete_index(
        self,
        not_exists_ok: bool = True,
        is_unique: bool = False,
        index_name: str | None = None,
        time_log: utils.LogLike | None = None,
    ):
        """インデックス削除"""
        column_name_list = [self.name]

        query_builder = IndexQueryBuilder(self.driver)
        head_query = query_builder.get_delete_head_query(not_exists_ok)
        _index_name = query_builder.get_index_name(column_name_list, index_name, is_unique)

        delete_index = head_query + " " + _index_name

        delete_index.execute(time_log=time_log)
        delete_index.commit(time_log=time_log)

    def set_foreign_key(self, references: Self):
        self.constraint.references = references
