from yai.entry import Log, dataclass, field, Path, datetime, base_utils_module
from yai.entry.sql_module.exceptions import ConstraintConflictError
from ..column.column import Column, ColumnConstraint
from yai.entry.sql_module.sqlite.constraint import CompositeConstraint


@dataclass
class CreateQueryBuilder:
    log: Log = field(default_factory=Log)

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

        for column in column_list:
            one_column_define_constraint_query = self._get_one_column_define_constraint_query(column)
            one_column_define_constraint_query_list.append(one_column_define_constraint_query)

        column_define_constraint_query = base_utils_module.str_.join_comma(one_column_define_constraint_query_list)
        return column_define_constraint_query

    def _get_one_column_define_constraint_query(self, column: Column) -> str:
        """
        1つの列定義・列制約のクエリ作成

        Column(name.now = 'text', constraint.python_type = str, constraint.not_null = True)
        ->
        'text TEXT NOT NULL'



        """
        sqlite_type = self._get_sqlite_type(column.constraint.python_type)  # "TEXT"など
        constraint_query = self._get_constraint_query(column.constraint)

        one_column_define_constraint_query = base_utils_module.str_.join_space(
            [column.name.now, sqlite_type, constraint_query], no_empty=True
        )
        return one_column_define_constraint_query

    def _get_sqlite_type(self, python_type: type) -> str:
        """
        str -> 'TEXT'
        int -> 'INTEGER'
        みたいな
        """
        # 型
        if python_type == str:
            return "TEXT"
        if python_type == bool:
            return "INTEGER"
        if python_type == int:
            return "INTEGER"
        if python_type == datetime.datetime:
            return "TEXT"
        if python_type == datetime.date:
            return "TEXT"
        if python_type == Path:
            return "TEXT"
        if python_type == bytes:
            return "BLOB"

        self.log.error(f"{python_type}はSQLの型に変換できません。")
        raise TypeError

    def _get_constraint_query(self, constraint: ColumnConstraint) -> str:
        """
        constraint.unique = True, constraint.not_null = True
        ->
        'UNIQUE NOT NULL'
        """
        # TODO default_valueする
        constraint_str_list = []
        # 主キーのとき
        if constraint.primary:
            constraint_str_list.append("PRIMARY KEY")
        # unique
        if constraint.unique:
            if constraint.primary:
                self.log.error("primaryキー制約とuniqueキー制約を同時に入れることはできません。")
                raise ConstraintConflictError
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
            constraint_str_list.append(f"DEFAULT {constraint.default_value}")

        constraint_query = base_utils_module.str_.join_space(constraint_str_list)
        return constraint_query

    def get_composite_constraint_query(self, composite_constraint_list: list[CompositeConstraint] | None) -> str:
        """
        [UNIQUECompositeConstraint([Column(name.now='site_id'), Column(name.now='content_id')]),
        UNIQUECompositeConstraint([Column(name.now='post_id'), Column(name.now='name')])]
        ->
        'UNIQUE (site_id, content_id), UNIQUE (post_id, name)'
        """
        if composite_constraint_list is None:
            composite_constraint_list = []
        composite_constraint_query_list = [
            one_composite_constraint_query.get_query() for one_composite_constraint_query in composite_constraint_list
        ]
        composite_constraint_query = base_utils_module.str_.join_comma(composite_constraint_query_list)

        return composite_constraint_query
