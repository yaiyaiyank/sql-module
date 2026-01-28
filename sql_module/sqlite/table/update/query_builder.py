# 標準ライブラリ
from dataclasses import dataclass

from sql_module import utils, Driver, Field, wheres, Query, query_join_comma


@dataclass
class UpdateQueryBuilder:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_head_query(self) -> Query:
        """UPDATE句"""
        return Query("UPDATE", driver=self.driver)

    def get_set_query(self, record: list[Field]) -> Query:
        """
        SET句
        """
        set_query_list = []
        set_query = Query()
        for field_ in record:
            eq_query = self._get_field_eq_query(field_)
            set_query_list.append(eq_query)

        all_eq_query = query_join_comma(set_query_list)
        set_query = "SET " + all_eq_query
        return set_query

    def _get_field_eq_query(self, field: Field) -> Query:
        """name = :p1みたいな部分"""
        query = Query()
        query += f"{field.column.name.now} = "
        query *= field.sql_value
        return query

    def get_where_query(self, where: wheres.Where | None = None) -> Query:
        """
        WHERE句
        """
        if where is None:
            where = wheres.TRUE()

        where_query = "WHERE " + where

        return where_query
