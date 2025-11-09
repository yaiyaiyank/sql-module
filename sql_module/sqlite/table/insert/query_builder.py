from yai.entry import Log, dataclass, field, Path, datetime, base_utils_module
from yai.entry.sql_module.exceptions import ConstraintConflictError
from yai.entry.sql_module.sqlite import Column, Field

from ...placeholder import PlaceHolderable


@dataclass
class InsertQueryBuilder(PlaceHolderable):
    @staticmethod
    def get_head_query() -> str:
        """最初のクエリ作成"""
        return "INSERT INTO"

    def get_value_query(self, record: list[Field]) -> str:
        keys_query = self._get_keys_query(record)
        place_holder_keys_query = self._get_place_holder_keys_query_query(record)
        return f"({keys_query}) VALUES ({place_holder_keys_query})"

    def _get_keys_query(self, record: list[Field]) -> str:
        # record -> ['name', 'age']
        keys = [field_.column.name.now for field_ in record]
        # ['name', 'age'] -> 'name, age'
        keys_query = base_utils_module.str_.join_comma(keys)
        return keys_query

    def _get_place_holder_keys_query_query(self, record: list[Field]) -> str:
        # record -> [':name', ':age']
        place_holder_keys = []
        for field_ in record:
            place_holder_key = self._add_placeholder(field_.sql_value)
            place_holder_keys.append(place_holder_key)
        # [':name', ':age'] -> ':name, :age'
        place_holder_keys_query = base_utils_module.str_.join_comma(place_holder_keys)
        return place_holder_keys_query

    def get_on_conflict_query(self, record: list[Field]) -> str:
        """
        conflict句の部分のクエリ

        kをupsertがTrueのfieldの数、nをfieldの数とする。ちなみに、以下がなりたつ。
        0 <= k <= n
        """
        # [Field(column.name.now = 'site_id', upsert=True), Field(column.name.now = 'content_id', upsert=True), Field(column.name.now = 'title')]
        # ->
        # on_conflict_keys = ['site_id', 'content_id'], on_conflict_keys = ['title']
        on_conflict_keys = [field_.column.name.now for field_ in record if field_.upsert]
        non_conflict_keys = [field_.column.name.now for field_ in record if not field_.upsert]
        # k == 0
        if on_conflict_keys.__len__() == 0:
            return ""
        # k < 0
        on_conflict_keys_query = self._get_on_conflict_keys_query(on_conflict_keys)
        ## k == n
        if on_conflict_keys.__len__() == non_conflict_keys.__len__():
            return f"ON CONFLICT ({on_conflict_keys_query}) DO NOTHING"
        ## k < n
        non_conflict_keys_query = self._get_non_conflict_keys_query(on_conflict_keys)
        return f"ON CONFLICT ({on_conflict_keys_query}) DO UPDATE SET {non_conflict_keys_query}"

    def _get_on_conflict_keys_query(self, on_conflict_keys: list[str]) -> str:
        # ['site_id', 'content_id'] -> 'site_id, content_id'
        on_conflict_keys_query = base_utils_module.str_.join_comma(on_conflict_keys)
        return on_conflict_keys_query

    def _get_non_conflict_keys_query(self, non_conflict_keys: list[str]) -> str:
        # ['title'] -> ['title = excluded.title']
        non_conflict_key_query_list = [
            f"{non_conflict_key} = excluded.{non_conflict_key}" for non_conflict_key in non_conflict_keys
        ]
        on_conflict_keys_query = base_utils_module.str_.join_comma(non_conflict_key_query_list)
        return on_conflict_keys_query
