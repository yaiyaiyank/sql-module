# 標準ライブラリ
from dataclasses import dataclass

from sql_module import utils, Field
from sql_module.sqlite.placeholder import PlaceHolderable


@dataclass
class UpdateQueryBuilder(PlaceHolderable):
    @staticmethod
    def get_head_query() -> str:
        """UPDATE句"""
        return "UPDATE"

    def get_set_query(self, record: list[Field]) -> str:
        """
        SET句

        [Field(column.name.now='name', value='Bob'), Field(column.name.now='age', value=25)]
        ->
        'name = :p0, age = :p1'
        """
        set_query_list = []
        for field_ in record:
            placeholder_key = self._add_placeholder(field_.sql_value)
            set_query_list.append(f"{field_.column.name.now} = {placeholder_key}")
        set_query = utils.join_comma(set_query_list)
        return set_query

    def get_where_query(self, record: list[Field] | None) -> str:
        """
        WHERE句

        [Field(column.name.now='id', value=10)]
        ->
        'WHERE id = :p2'
        """
        if record is None or record.__len__() == 0:
            return ""
        where_query_list = []
        for field_ in record:
            placeholder_key = self._add_placeholder(field_.sql_value)
            where_query_list.append(f"{field_.column.name.now} = {placeholder_key}")
        where_query = " AND ".join(where_query_list)
        return f"WHERE {where_query}"
