from pathlib import Path
from dataclasses import dataclass
import datetime

from sql_module.sqlite.table.column.interface import ColumnLike


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
