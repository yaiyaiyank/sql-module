from pathlib import Path
from dataclasses import dataclass
import datetime

from sql_module.sqlite.table.column.interface import ColumnLike
from sql_module.exceptions import ConstraintConflictError, SQLTypeError


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

        raise SQLTypeError(f"値の型: {self.python_type}はSQLの型に変換できません。")

    @property
    def sql_default_value(self):
        """
        create時のデフォルト値をsqlのデフォルト値に変換する。

        createだけ値がプレースホルダ対応していないだけで、
        insert, update, selectは値はちゃんとプレースホルダを使っているので、SQLインジェクションみたいなのは起こらない
        """
        if self.default_value is None:
            raise TypeError("sql_default_valueを呼び出すときにdefault_valueがNoneだとダメ！！！")

        if self.python_type in [datetime.date, datetime.datetime]:
            # CURRENT_TIMESTAMPの場合
            if self.default_value == "CURRENT_TIMESTAMP":
                sql_value = self.default_value
                return sql_value
            # 非対応
            if not isinstance(self.default_value, datetime.date):
                raise TypeError("sqliteにそのdatetime.date系オブジェクトは対応していません。")
            # sqliteの日付()へ変換
            if isinstance(self.default_value, datetime.datetime):
                iso_date = self.default_value.isoformat(" ")
                sql_value = f"'{iso_date}'"
                return sql_value
            if isinstance(self.default_value, datetime.date):
                iso_date = datetime.datetime.combine(self.default_value, datetime.time()).isoformat(" ")
                sql_value = f"'{iso_date}'"
                return sql_value

        # 文字列やパスの場合はクォーテーションが必要
        if self.python_type in [str, Path]:
            # (type, value)の組み合わせが(str, str), (str, Path), (Path, str), (Path, Path)でok
            if not isinstance(self.default_value, str | Path):
                raise TypeError("sqliteにそのstr系オブジェクトは対応していません。")
            sql_value = f"'{self.default_value}'"
            return sql_value

        # BLOBの場合はhexしてX&クォーテーションが必要
        if self.python_type in [bytes]:
            if not isinstance(self.default_value, bytes):
                raise TypeError("sqliteにそのbytes系オブジェクトは対応していません。")
            hex_str = self.default_value.hex().upper()
            sql_value = f"X'{hex_str}'"
            return sql_value

        # intやBLOBの場合
        if self.python_type in [int, bool]:
            # (type, value)の組み合わせが(int, int), (int, bool), (Path, bool), (bool, bool)でok。boolはintのサブクラス。
            if not isinstance(self.default_value, int):
                raise TypeError("sqliteにそのbytes系オブジェクトは対応していません。")

            if isinstance(self.default_value, bool):
                sql_value = int(self.default_value)  # True -> 1, False -> 0
                return sql_value
            # int
            sql_value = self.default_value
            return sql_value
