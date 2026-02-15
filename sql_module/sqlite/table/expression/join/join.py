# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime
from typing import Self, Literal

from sql_module import Column, Query, query_join_comma, expressions


class Join(Query):
    def __init__(
        self,
        column: Column,
        is_references_table: bool = True,
        type: Literal["inner", "left", "right", "full"] = "inner",
        target_column: Column | None = None,
    ):
        if target_column is None:
            self.raise_for_references_column(column)
            target_column = column.constraint.references

        query = Query()
        query += self.get_join_type_query(type)
        query += " "
        query += self.get_join_table_name(column, target_column, is_references_table)
        query += f" ON {target_column.name} = {column.name}"

        self.straight_set(query)

    def get_join_type_query(self, join_type: Literal["inner", "left", "right", "full"]):
        if join_type == "inner":
            return Query("JOIN")
        if join_type == "left":
            return Query("LEFT JOIN")
        if join_type == "left":
            return Query("RIGHT JOIN")
        if join_type == "full":
            return Query("FULL JOIN")
        raise ValueError("Joinタイプが違います")

    def get_join_table_name(self, column: Column, target_column: Column, is_references_table: bool) -> str:
        # join先が外部キー
        if is_references_table:
            join_column = target_column
        # join先が自分
        else:
            join_column = column

        return join_column.name.table_name

    def raise_for_references_column(self, column: Column):
        # リファレンスのみ
        if column.constraint.references is None:
            raise ValueError("target_column引数なしでJOINできるのは外部キー制約のカラムのみです。")
