from dataclasses import dataclass, field
from sql_module.sqlite.driver import Driver
from typing import Self
from pathlib import Path
import datetime


class Query:
    def __init__(self, first_string: str | None = None, driver: Driver = None):
        """
        sqlのクエリを司るオブジェクト。+で文字結合, *で値結合して使う
        # TODO Python3.14からテンプレート文字列というのがあるらしい。これ使えるんじゃね...?
        """
        self.driver = driver

        # 最初の文字を設定
        if first_string is None:
            first_string = ""
        self.string_list = [first_string]
        # 最初の値リストを設定(空のリスト)
        self.value_list = []

    def raise_for_length(self):
        if not self.value_list.__len__() + 1 == self.string_list.__len__():
            raise ValueError(
                f"value_listの長さ: {self.value_list.__len__()}, string_listの長さ: {self.string_list.__len__()}"
            )

    def copy(self) -> Self:
        """自Queryオブジェクトのコピーを取得"""
        _query = Query()
        _query.string_list = self.string_list.copy()
        _query.value_list = self.value_list.copy()
        _query.driver = self.driver  # driverはコピーして別インスタンスにしてはいけない
        return _query

    def straight_set(self, query: Self):
        """他Queryオブジェクトを自Queryオブジェクトに上書き"""
        _query = query.copy()
        self.string_list = _query.string_list
        self.value_list = _query.value_list
        self._merge_driver(_query.driver)

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
            # selfかotherにdriverがあったら採用する
            self._merge_driver(other.driver)

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

        self.string_list.insert(0, "")
        self.value_list.insert(0, other)
        return self

    def __str__(self) -> str:
        query_string, placeholder_dict = self.measurement()
        return query_string

    def __repr__(self):
        query_string, placeholder_dict = self.measurement()
        if self.driver is None:
            driver_string = None
        else:
            driver_string = self.driver.database_file_path
        return f"string: {query_string}\nplaceholder_dict: {placeholder_dict}\ndb_path: {driver_string}"

    def _merge_driver(self, other_driver: Self):
        # 演算時にdriverを注入する
        # selfとotherのdriverが取りうるのはNoneか同じIDのdriver。上書きok。
        if not other_driver is None:
            self.driver = other_driver
            return
        if not hasattr(self, "driver"):
            self.driver = None
            return

    def execute(self):
        if self.driver is None:
            raise ValueError("driver属性が無いです。")
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

        query_string = query_string.strip()

        return query_string, placeholder_dict


def query_join_comma(query_list: list[Query], no_empty: bool = False) -> Query:
    base_query = Query()
    # :p0, :p1
    i = 0
    for query in query_list:
        if no_empty and query.string_list.__len__() == 1 and query.string_list[0] == "":
            continue
        # 初回のみカンマいらぬ
        if i > 0:
            base_query += ", "
        i += 1
        base_query += query

    return base_query
