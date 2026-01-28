# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime

from sql_module import utils, Driver, Column, Query, query_join_comma, CompositeConstraint
from sql_module.sqlite.table.column.column import ColumnConstraint

from sql_module.exceptions import ConstraintConflictError, SQLTypeError


class CreateQueryBuilder:
    def __init__(self, driver: Driver):
        self.driver = driver

    def get_head_query(self, exists_ok: bool) -> Query:
        """最初のクエリ作成"""
        if exists_ok:
            return Query("CREATE TABLE IF NOT EXISTS", driver=self.driver)
        return Query("CREATE TABLE", driver=self.driver)

    def get_column_define_constraint_query(self, column_list: list[Column]) -> Query:
        """
        列定義・列制約のクエリ作成
        [Column(name.now = 'create_dt', constraint.python_type = datetime.date, constraint.not_null = True),
        Column(name.now = 'post_id', constraint.python_type = int, constraint.unique = True, constraint.not_null = True)]
        ->
        'create_dt TIMESTAMP NOT NULL, '

        """
        one_column_define_constraint_query_list = []

        # 複数のprimaryはPrimaryCompositeConstraintを使うべし。個人的には非推奨ですが。。。

        for column in column_list:
            one_column_define_constraint_query = self._get_one_column_define_constraint_query(column)
            one_column_define_constraint_query_list.append(one_column_define_constraint_query)

        column_define_constraint_query = utils.join_comma(one_column_define_constraint_query_list)
        return Query(column_define_constraint_query)

    def _get_one_column_define_constraint_query(self, column: Column) -> str:
        """
        1つの列定義・列制約のクエリ作成

        Column(name.now = 'text', constraint.python_type = str, constraint.not_null = True)
        ->
        'text TEXT NOT NULL'
        """
        constraint_query = self._get_constraint_query(column.constraint)

        one_column_define_constraint_query = utils.join_space(
            [column.name.now, column.constraint.sql_type, constraint_query], no_empty=True
        )
        return one_column_define_constraint_query

    def _get_constraint_query(self, constraint: ColumnConstraint) -> str:
        """
        constraint.unique = True, constraint.not_null = True
        ->
        'UNIQUE NOT NULL'
        """
        constraint_str_list = []
        # 主キーのとき
        if constraint.primary:
            constraint_str_list.append("PRIMARY KEY")
        # unique
        if constraint.unique:
            if constraint.primary:
                raise ConstraintConflictError("primaryキー制約とuniqueキー制約を同時に入れることはできません。")
            constraint_str_list.append("UNIQUE")
        # not null
        if constraint.not_null:
            constraint_str_list.append("NOT NULL")
        # references
        if not constraint.references is None:
            constraint_str_list.append(
                f"REFERENCES {constraint.references.name.table_name.now} ({constraint.references.name.now}) ON DELETE CASCADE"
            )
        # default
        if not constraint.default_value is None:
            constraint_str_list.append(f"DEFAULT {constraint.sql_default_value}")

        constraint_query = utils.join_space(constraint_str_list)
        return constraint_query

    def get_composite_constraint_query(
        self, composite_constraint_list: list[CompositeConstraint] | CompositeConstraint | None
    ) -> Query:
        """
        [UNIQUECompositeConstraint([Column(name.now='site_id'), Column(name.now='content_id')]),
        UNIQUECompositeConstraint([Column(name.now='post_id'), Column(name.now='name')])]
        ->
        'UNIQUE (site_id, content_id), UNIQUE (post_id, name)'
        """
        if composite_constraint_list is None:
            composite_constraint_list = []
        if isinstance(composite_constraint_list, CompositeConstraint):
            composite_constraint_list = [composite_constraint_list]
        composite_constraint_query_list = [
            one_composite_constraint_query.get_query() for one_composite_constraint_query in composite_constraint_list
        ]
        composite_constraint_query = utils.join_comma(composite_constraint_query_list)

        return Query(composite_constraint_query)
