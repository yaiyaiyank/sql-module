# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime

from sql_module import utils
from sql_module.sqlite.table.column.column import Column, ColumnConstraint
from sql_module import CompositeConstraint

from sql_module.exceptions import ConstraintConflictError, SQLTypeError


class CreateQueryBuilder:
    @staticmethod
    def get_head_query(exists_ok: bool) -> str:
        """最初のクエリ作成"""
        if exists_ok:
            return "CREATE TABLE IF NOT EXISTS"
        return "CREATE TABLE"

    def get_column_define_constraint_query(self, column_list: list[Column]) -> str:
        """
        列定義・列制約のクエリ作成
        [Column(name.now = 'create_dt', constraint.python_type = datetime.date, constraint.not_null = True),
        Column(name.now = 'post_id', constraint.python_type = int, constraint.unique = True, constraint.not_null = True)]
        ->
        'create_dt TIMESTAMP NOT NULL, '

        """
        one_column_define_constraint_query_list = []

        # 複数のprimaryはPrimaryCompositeConstraintを使うべし。個人的には非推奨ですが。。。
        pass

        for column in column_list:
            one_column_define_constraint_query = self._get_one_column_define_constraint_query(column)
            one_column_define_constraint_query_list.append(one_column_define_constraint_query)

        column_define_constraint_query = utils.join_comma(one_column_define_constraint_query_list)
        return column_define_constraint_query

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
            self._get_default_query(constraint.python_type, constraint.default_value)
            constraint_str_list.append(f"DEFAULT {constraint.sql_default_value}")

        constraint_query = utils.join_space(constraint_str_list)
        return constraint_query

    def _get_default_query(self, python_type: type, default_value: str | int | bytes | Path | datetime.date) -> str:
        # datetime.datetime
        if python_type in [datetime.date, datetime.datetime]:
            # CURRENT_TIMESTAMPの場合
            if default_value == "CURRENT_TIMESTAMP":
                default_query = f"DEFAULT {default_value}"
                return default_query
            # 非対応
            if not isinstance(default_value, datetime.date):
                raise TypeError("sqliteにそのdatetime.date系オブジェクトは対応していません。")
            # sqliteの日付へ変換
            if isinstance(default_value, datetime.datetime):
                iso_datetime = default_value.isoformat(" ")
                return iso_datetime
            if isinstance(default_value, datetime.date):
                iso_datetime = datetime.datetime.combine(default_value, datetime.time()).isoformat(" ")
                return iso_datetime

        # BLOBの場合はクォーテーションが必要
        if python_type == bytes:
            hex_str = default_value.hex().upper()
            return f"DEFAULT X'{hex_str}'"
        # 文字列やパスの場合はクォーテーションが必要
        if python_type in [str, Path]:
            default_query = f"DEFAULT '{default_value}'"
            return default_query

    def get_composite_constraint_query(
        self, composite_constraint_list: list[CompositeConstraint] | CompositeConstraint | None
    ) -> str:
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

        return composite_constraint_query
