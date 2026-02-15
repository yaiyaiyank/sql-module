# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime
from typing import Self, Literal

from sql_module import Column, Query, query_join_comma, expressions


class OrderBy(Query):
    def __init__(self, column: Column, is_asc: bool = True):
        query = Query()
        query += f"{column.name} {self.get_sc(is_asc)}"

        self.straight_set(query)

    def get_sc(self, is_asc: bool) -> str:
        if is_asc:
            return "ASC"
        return "DESC"
