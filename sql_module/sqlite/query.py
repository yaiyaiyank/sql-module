from dataclasses import dataclass, field
from sql_module.sqlite.driver import Driver
from typing import Self
import copy
from pathlib import Path
import datetime


@dataclass
class Querable:
    driver: Driver
    query: str
    placeholder_dict: dict = field(default_factory=dict)

    def execute(self):
        self.driver.execute(self.query, self.placeholder_dict)


@dataclass
class Query:
    driver: Driver = None
    string_list: list[str] = field(default_factory=lambda: [""])
    value_list: list = field(default_factory=list)

    def raise_for_length(self):
        if not self.value_list.__len__() + 1 == self.string_list.__len__():
            raise ValueError(
                f"value_listの長さ: {self.value_list.__len__()}, string_listの長さ: {self.string_list.__len__()}"
            )

    def __add__(self, other: str | Self) -> Self:
        self = self.copy()
        if isinstance(other, str):
            self.string_list[-1] = f"{self.string_list[-1]}{other}"
            return self
        if isinstance(other, Query):
            other = other.copy()
            # stringのselfとotherをつなげる
            self.string_list[-1] = f"{self.string_list[-1]}{other.string_list[0]}"
            self.string_list += other.string_list[1:]  # 要素数が1でも ["aaa"][1:] -> [] ってなるので問題ないです。
            # valueのselfとotherをつなげる
            self.value_list += other.value_list

            return self

    def __radd__(self, other: str) -> Self:
        self = self.copy()
        self.string_list[0] = f"{other}{self.string_list[0]}"
        return self

    def __mul__(self, other: str | int | bytes | Path | datetime.date | None) -> Self:
        self = self.copy()
        if isinstance(other, Query):
            TypeError("Query*Queryは対応していません。Query+Queryは対応しています。")

        self.string_list.append("")
        self.value_list.append(other)
        return self

    def __rmul__(self, other: str | int | bytes | Path | datetime.date | None) -> Self:
        self = self.copy()
        if isinstance(other, Query):
            TypeError("Query*Queryは対応していません。Query+Queryは対応しています。")

        self.string_list.insert(0, "")
        self.value_list.insert(0, other)
        return self

    def __str__(self) -> str:
        query_string, placeholder_dict = self.measurement()
        return query_string

    def execute(self):
        if self.driver is None:
            ValueError("実行するクエリにdriverを入れてください。")
        query_string, placeholder_dict = self.measurement()
        self.driver.execute(query_string, placeholder_dict)

    def measurement(self) -> tuple[str, dict]:
        """現時点のクエリを観測"""
        self.raise_for_length()
        query_string = self.string_list[0]
        string_list2 = self.string_list[1:]
        placeholder_dict = dict()

        for i, (value, string) in enumerate(zip(self.value_list, string_list2)):
            # 変数名 (例: ':p2')
            value_name = f"p{i}"
            query_string += f":{value_name}{string}"
            placeholder_dict[value_name] = value

        return query_string, placeholder_dict

    def copy(self) -> Self:
        """
        自身のインスタンスをコピーする
        これを介せずにquery1 + query2とかをすると、query1やquery2が別の値になってしまう
        """
        query = copy.copy(self)
        return query
