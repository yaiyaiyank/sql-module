# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime

from sql_module import utils, Driver, Field, Query, query_join_comma
from sql_module.exceptions import FetchNotFoundError


class Insert(Query):
    def fetch_id(self) -> int:
        """insert後にidをfetch"""
        try:
            id_dict = self.driver.fetchone()
            return id_dict["id"]
        except FetchNotFoundError:
            if not "RETURNING id" in self.__str__():
                raise FetchNotFoundError("insert時の引数でis_returning_idをTrueにしてください")
            if "DO NOTHING" in self.__str__():
                # 'DO NOTHING'の文字は値に含まれているかもしれないが、それはプレースホルダなのでこの分岐で問題ないと判断
                raise FetchNotFoundError("全てのカラムがon conflictな場合、insertでidを取得できません。")
            raise


@dataclass
class InsertQueryBuilder:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_head_query(self) -> Query:
        """最初のクエリ作成"""
        return Query("INSERT INTO", driver=self.driver)

    def get_value_query(self, record: list[Field] | Field) -> Query:
        if isinstance(record, Field):
            record = [record]
        keys = self._get_keys(record)
        place_holder_keys_query = self._get_place_holder_keys_query_query(record)
        return f"({keys}) VALUES (" + place_holder_keys_query + ")"

    def _get_keys(self, record: list[Field]) -> str:
        # record -> ['name', 'age']
        keys = [field_.column.name.now for field_ in record]
        # ['name', 'age'] -> 'name, age'
        keys = utils.join_comma(keys)
        return keys

    def _get_place_holder_keys_query_query(self, record: list[Field]) -> Query:
        # record -measurement> [':p0, :p1'], ["Alice", 10]
        query_list = []
        for field_ in record:
            query = Query()
            query *= field_.sql_value
            query_list.append(query)
        place_holder_keys_query = query_join_comma(query_list)
        return place_holder_keys_query

    def get_on_conflict_query(self, record: list[Field] | Field) -> Query:
        """
        conflict句の部分のクエリ

        kをupsertがTrueのfieldの数、nをfieldの数とする。ちなみに、以下がなりたつ。
        0 <= k <= n
        """
        if isinstance(record, Field):
            record = [record]
        # [Field(column.name.now = 'site_id', upsert=True), Field(column.name.now = 'content_id', upsert=True), Field(column.name.now = 'title')]
        # ->
        # on_conflict_keys = ['site_id', 'content_id'], non_conflict_keys = ['title']
        on_conflict_keys = [field_.column.name.now for field_ in record if field_.upsert]
        non_conflict_keys = [field_.column.name.now for field_ in record if not field_.upsert]
        # k == 0
        if on_conflict_keys.__len__() == 0:
            return Query()
        # k < 0
        on_conflict_keys_query = self._get_on_conflict_keys_query(on_conflict_keys)
        ## k == n
        if on_conflict_keys.__len__() == record.__len__():
            return Query(f"ON CONFLICT ({on_conflict_keys_query}) DO NOTHING")
        ## k < n
        non_conflict_keys_query = self._get_non_conflict_keys_query(non_conflict_keys)
        return Query(f"ON CONFLICT ({on_conflict_keys_query}) DO UPDATE SET {non_conflict_keys_query}")

    def _get_on_conflict_keys_query(self, on_conflict_keys: list[str]) -> str:
        # ['site_id', 'content_id'] -> 'site_id, content_id'
        on_conflict_keys_query = utils.join_comma(on_conflict_keys)
        return on_conflict_keys_query

    def _get_non_conflict_keys_query(self, non_conflict_keys: list[str]) -> str:
        # ['title'] -> ['title = excluded.title']
        non_conflict_key_query_list = [
            f"{non_conflict_key} = excluded.{non_conflict_key}" for non_conflict_key in non_conflict_keys
        ]
        on_conflict_keys_query = utils.join_comma(non_conflict_key_query_list)
        return on_conflict_keys_query

    def get_returning_id_query(self, is_returning_id: bool = False) -> Query:
        if is_returning_id:
            return Query("RETURNING id")

        return Query()
