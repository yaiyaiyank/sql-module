# 標準ライブラリ
from dataclasses import dataclass
import sqlite3
from typing import Literal
from pathlib import Path
import datetime

from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint

from sql_module import (
    utils,
    Driver,
    Field,
    conds,
    Query,
    query_join_comma,
    query_join_space,
    Column,
    funcs,
    Join,
    OrderBy,
    expressions,
)
from sql_module.sqlite.table.sub_query import SubQuery


@dataclass
class SelectType:
    """サブクエリ用のselectの情報"""

    scalarzation: bool = False  # 一行かどうか


class Select(SubQuery):
    def set_select_type(self, select_type: SelectType):
        self.select_type = select_type

    def fetchall(
        self, dict_output: bool = False, time_log: utils.LogLike | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        return self.driver.fetchall(dict_output, time_log=time_log)

    def fetchmany(
        self, limit: int, dict_output: bool = False, time_log: utils.LogLike | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        return self.driver.fetchmany(limit, dict_output, time_log=time_log)

    def fetchgrid(
        self, limit: int, dict_output: bool = False, time_log: utils.LogLike | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        return self.driver.fetchgrid(limit, dict_output, time_log=time_log)

    def fetchone(self, dict_output: bool = False, time_log: utils.LogLike | None = None) -> dict[str] | sqlite3.Row:
        return self.driver.fetchone(dict_output, time_log=time_log)

    def fetchall_value_list(self, time_log: utils.LogLike | None = None) -> list[int | str | None]:
        """単一カラムのfetchした値のリストを取得"""
        fetchall = self.driver.fetchall(time_log=time_log)
        return [fetch[0] for fetch in fetchall]

    def fetchmany_value_list(self, limit: int, time_log: utils.LogLike | None = None) -> list[int | str | None]:
        """単一カラムのfetchした値のリストを取得"""
        fetchmany = self.driver.fetchmany(limit, time_log=time_log)
        return [fetch[0] for fetch in fetchmany]

    def fetchgrid_value_list(self, limit: int, time_log: utils.LogLike | None = None) -> list[list[int | str | None]]:
        all_list = []
        while True:
            fetchmany_value_list = self.fetchmany_value_list(limit, time_log=time_log)
            # fetchmany_listがなくなるまで
            if fetchmany_value_list.__len__() == 0:
                break

            all_list.append(fetchmany_value_list)

        return all_list

    def fetchone_value(self, time_log: utils.LogLike | None = None) -> int | str | None:
        """単一カラムのfetchした値を取得"""
        fetchone = self.driver.fetchone(time_log=time_log)
        return fetchone[0]


class SelectQueryBuilder:
    def __init__(self, driver: Driver):
        self.driver = driver
        self.select_type = SelectType()

    def get_head_query(self) -> Query:
        """SELECT句"""
        return Query("SELECT", driver=self.driver)

    def get_expression_query(
        self,
        expression: list[expressions.Expression | Literal[1]] | expressions.Expression | Literal[1] | None,
    ) -> Query:

        if expression is None:
            return Query("*")

        if not isinstance(expression, list):
            expression = [expression]
        else:
            expression = expression.copy()

        expression_element_query_list = []  # Selectのexpression部分のクエリ作成に使う
        for expression_element in expression:
            expression_element_query = self._get_expression_element_query(expression_element)
            expression_element_query_list.append(expression_element_query)

        expression_query = query_join_comma(expression_element_query_list)

        return expression_query

    def _get_expression_element_query(self, expression_element: expressions.Expression | Literal[1]):
        """expressionの各要素"""
        # 1はconds.TRUEと同等で、しかもExpressionにしたいので
        if expression_element == 1:
            expression_element = conds.TRUE()

        # 1行にしてしまうものが含まれていたら一旦True。そのうちGROUP BY実装時に微調整できるようにする
        if isinstance(expression_element, expressions.ScalarzationExpression):
            self.select_type.scalarzation = True

        # カラムをQueryオブジェクトとする
        if isinstance(expression_element, Column):
            query = Query(expression_element.name.__str__())
            return query

        # その他そのままreturnするExpression兼Queryオブジェクト
        if isinstance(expression_element, expressions.Expression):
            return expression_element

        # strな場合
        if isinstance(expression_element, str):
            query = Query(expression_element)
            return query

        # 自作Queryな場合
        if isinstance(expression_element, Query):
            return expression_element

        # raise
        if isinstance(expression_element, int):
            raise ValueError(
                "SELECT文の式で、数値は1のみ対応で、1を指定した場合は'SELECT 1 ~'となります。'SELECT 2 ~'をどうしてもやりたい場合、直接driver.executeしてください。"
            )
        raise ValueError(f"SELECT文の式で、非対応な値が入力されました。式: {expression_element}")

    def get_from_query(self, is_from: bool, table_name: str) -> Query:
        # そのうちサブクエリ対応する
        # from句がない場合(Existsなど)、1つの値が返されるのでそうする
        if not is_from:
            self.select_type.scalarzation = True
            return Query()

        return Query(f"FROM {table_name}")

    def get_join_query(self, join: list[Join] | Join | None = None) -> Query:
        if join is None:
            return Query()
        if isinstance(join, Join):
            join_list = [join]
        if isinstance(join, list):
            join_list = join.copy()

        query = query_join_space(join_list)
        return query

    def get_where_query(self, where: conds.Cond | None = None) -> Query:
        """
        WHERE句
        """
        if where is None:
            return Query()

        where_query = "WHERE " + where

        return where_query

    def get_group_by_query(self, group_by: list[Column] | Column | None = None) -> Query:
        if group_by is None:
            return Query()
        if isinstance(group_by, Column):
            group_by_list = [group_by]
        if isinstance(group_by, list):
            group_by_list = group_by.copy()

        # group_byなら複数になる
        self.select_type.scalarzation = False

        order_by_list2 = [Query(column.name.__str__()) for column in group_by_list]

        group_by_query_base = query_join_comma(order_by_list2)
        group_by_query = "GROUP BY " + group_by_query_base
        return group_by_query

    def get_having_query(self, having: conds.Cond | None = None) -> Query:
        if having is None:
            return Query()

        having_query = "HAVING " + having

        return having_query

    def get_order_by_query(self, order_by: list[OrderBy] | OrderBy | None = None) -> Query:
        if order_by is None:
            return Query()
        if isinstance(order_by, OrderBy):
            order_by_list = [order_by]
        if isinstance(order_by, list):
            order_by_list = order_by.copy()

        order_by_query_base = query_join_comma(order_by_list)
        order_by_query = "ORDER BY " + order_by_query_base
        return order_by_query

    def get_limit_query(self, limit: int | None = None) -> Query:
        if limit is None:
            return Query()

        return Query(f"LIMIT {limit}")
