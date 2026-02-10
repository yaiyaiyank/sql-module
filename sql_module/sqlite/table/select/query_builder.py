# 標準ライブラリ
from dataclasses import dataclass
import sqlite3
from typing import Literal

from sql_module import utils, Driver, Field, wheres, Query, query_join_comma, Column
from sql_module.sqlite.table.name import TableName


class Select(wheres.Where):
    def fetchall(
        self, dict_output: bool = False, time_log: Literal["print_log"] | utils.PrintLog | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        return self.driver.fetchall(dict_output, time_log=time_log)

    def fetchmany(
        self, limit: int, dict_output: bool = False, time_log: Literal["print_log"] | utils.PrintLog | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        return self.driver.fetchmany(limit, dict_output, time_log=time_log)

    def fetchone(
        self, dict_output: bool = False, time_log: Literal["print_log"] | utils.PrintLog | None = None
    ) -> dict[str] | sqlite3.Row:
        return self.driver.fetchone(dict_output, time_log=time_log)

    def exists(self):
        """selectのexists"""


@dataclass
class SelectQueryBuilder:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_head_query(self) -> Query:
        """SELECT句"""
        return Query("SELECT", driver=self.driver)

    def get_select_column_query(self, column_list: list[Column] | Column) -> Query:
        if isinstance(column_list, Column):
            column_list = [column_list]
        if column_list is None:
            select_column_query = "*"
        else:
            select_column_list = [column.name.get() for column in column_list]
            select_column_query = utils.join_comma(select_column_list)

        return Query(select_column_query)

    def get_from_query(self, table_name: TableName) -> Query:
        from_query = f"FROM {table_name.get()}"
        return Query(from_query)

    def get_where_query(self, where: wheres.Where | None = None) -> Query:
        """
        WHERE句
        """
        if where is None:
            where = wheres.TRUE()

        where_query = "WHERE " + where

        return where_query
