from pathlib import Path
from dataclasses import dataclass
import datetime
import sqlite3

from sql_module.sqlite.table.column.interface import ColumnLike
from sql_module import exceptions, get_sql_value


@dataclass
class ColumnConstraint:
    """
    列制約
    TODO Columnとinitにある型が相互依存している場合のベストプラクティスを知りたい。ちゃっぴーはColumnLikeというインターフェースを作れって言ってた
    """

    python_type: type
    unique: bool = False
    not_null: bool = False
    primary: bool = False  # AUTO_INCREMENTは廃止されました。そのうちuuid対応するかも
    references: ColumnLike | None = None  # ColumnLikeは別ファイルのColumnと相互依存しているために使っている
    default_value: str | int | bytes | Path | datetime.date | None = None  # bool, datetime.datetime内包

    @property
    def sql_type(self) -> str:
        """
        self.python_type = str -> 'TEXT'
        self.python_type = int -> 'INTEGER'
        みたいな

        """
        # 型
        if self.python_type in [str, Path, datetime.datetime, datetime.date]:
            return "TEXT"
        if self.python_type in [bool, int]:
            return "INTEGER"
        if self.python_type in [bytes]:
            return "BLOB"

        raise exceptions.SQLTypeError(f"値の型: {self.python_type}はSQLの型に変換できません。")

    @property
    def sql_default_value(self):
        """
        create時のデフォルト値をsqlのデフォルト値に変換する。

        createだけ値がプレースホルダ対応していないだけで、
        insert, update, selectは値はちゃんとプレースホルダを使っているので、SQLインジェクションみたいなのは起こらない
        """
        if self.default_value is None:
            raise TypeError("デフォルト値がNoneの場合は設定しなくても良いです")

        sql_default_value = self.get_sql_value(self.default_value, is_placeholder=False)

        return sql_default_value

    def get_sql_value(
        self, python_value: str | int | bytes | Path | datetime.date | None, is_placeholder: bool = True
    ) -> str | int | sqlite3.Binary | None:
        """
        カラム情報に応じてpythonの値をsqlの値に変換
        """
        return get_sql_value(python_value, self.python_type, is_not_null=self.not_null, is_placeholder=is_placeholder)
