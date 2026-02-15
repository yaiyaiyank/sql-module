from pathlib import Path
from dataclasses import dataclass
import datetime
import sqlite3

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
            raise TypeError("デフォルト値がNoneの場合は設定しなくても良いです")

        sql_default_value = self.get_sql_value(self.default_value, is_placeholder=False)

        return sql_default_value

    def get_sql_value(
        self, python_value: str | int | bytes | Path | datetime.date | None, is_placeholder: bool = True
    ) -> str | int | sqlite3.Binary | None:
        """
        pythonの値をsqlの値に変換
        is_placeholderはデフォルト値以外など、プレースホルダを使って入力しなければならないとき

        """
        if python_value is None:
            if self.not_null:
                raise ValueError("not_nullが適用されたカラムでNoneは挿入不可で、whereでも考慮する必要はないです。")
            return None

        if self.python_type in [datetime.date, datetime.datetime]:
            # CURRENT_TIMESTAMPの場合
            if python_value == "CURRENT_TIMESTAMP":
                if is_placeholder:
                    raise ValueError("プレースホルダで'CURRENT_TIMESTAMP'を使用できません。")
                sql_value = python_value  # むしろ "'CURRENT_TIMESTAMP'" でなくて良い
                return sql_value
            if isinstance(python_value, str):
                try:
                    datetime.datetime.strptime(python_value, "%Y-%m-%d %H:%M:%S")  # バリデーション
                    return self._get_placeholder_string(python_value, is_placeholder)
                except ValueError:
                    raise ValueError(
                        "sqliteのdatetime.date系カラムに入力できるISO形式の文字列は'%Y-%m-%d %H:%M:%S'形式です。"
                    )
            # 非対応 (文字列は上記"CURRENT_TIMESTAMP"しか対応しないので、"2024-01-01"の入力は受け付けない。代わりに、)
            if not isinstance(python_value, datetime.date):
                raise TypeError(
                    f"sqliteのdatetime.date系カラムに、入力した型: {python_value.__class__.__name__} は対応していません。"
                )
            # sqliteの日付()へ変換
            if isinstance(python_value, datetime.datetime):
                # microsecondやtimezoneがあったら取り除きながら datetime.datetime(2026, 1, 28, 3, 21, 53) -> '2026-01-28 03:21:53'
                sql_value = python_value.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
                return self._get_placeholder_string(sql_value, is_placeholder)

            if isinstance(python_value, datetime.date):
                # timezoneがあったら取り除きながら datetime.date(2026, 1, 28) -> '2026-01-28 00:00:00'
                sql_value = datetime.datetime.combine(python_value, datetime.time()).strftime("%Y-%m-%d %H:%M:%S")
                return self._get_placeholder_string(sql_value, is_placeholder)

        # 文字列やパスの場合はクォーテーションが必要
        if self.python_type in [str, Path]:
            # (type, value)の組み合わせが(str, str), (str, Path), (Path, str), (Path, Path)でok
            if not isinstance(python_value, str | Path):
                raise TypeError(
                    f"sqliteのstr系カラムに、入力した型: {python_value.__class__.__name__} は対応していません。"
                )
            return self._get_placeholder_string(python_value, is_placeholder)

        # BLOBの場合はhexしてX&クォーテーションが必要
        if self.python_type in [bytes]:
            if not isinstance(python_value, bytes):
                raise TypeError(
                    f"sqliteのbytes系カラムに、入力した型: {python_value.__class__.__name__} は対応していません。"
                )
            if is_placeholder:
                sql_value = sqlite3.Binary(python_value)
            else:
                hex_str = python_value.hex().upper()
                sql_value = f"X'{hex_str}'"
            return sql_value

        # intやBLOBの場合
        if self.python_type in [int, bool]:
            # (type, value)の組み合わせが(int, int), (int, bool), (bool, int), (bool, bool)でok。boolはintのサブクラス。
            if not isinstance(python_value, int):
                raise TypeError(
                    f"sqliteのint系カラムに、入力した型: {python_value.__class__.__name__} は対応していません。"
                )

            if isinstance(python_value, bool):
                sql_value = int(python_value)  # True -> 1, False -> 0
                return sql_value
            # int
            sql_value = python_value
            return sql_value

        raise SQLTypeError(
            f"その値の型: {type(python_value)} は、sqliteでいう型: {python_value.__class__.__name__} に対応する型に変換できません。"
        )

    def _get_placeholder_string(self, string: Path | str, is_placeholder: bool):
        if is_placeholder:
            return f"{string}"
        return f"'{string}'"
