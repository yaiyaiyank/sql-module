from yai.entry import *
import sqlite3


def python_value_to_sql_value(
    python_value: str | int | bytes | Path | datetime.date | None,
) -> str | int | bytes | None:
    """
    'aaa' -> 'aaa'
    2 -> 2
    True -> 1
    datetime.date(2025, 1, 1) -> '2025-01-01'
    datetime.datetime(2025, 1, 1) -> '2025-01-01 00:00:00'

    ちなみに、sqlite_value入れても問題ないです
    """
    if isinstance(python_value, str):
        return python_value
    elif isinstance(python_value, bool):  # bool型の場合はintへ (sqliteはbool非対応)
        return int(python_value)
    elif isinstance(python_value, int):  # boolはintのサブクラス注意
        return python_value
    elif isinstance(python_value, datetime.datetime):  # datetime型の場合はISO 8601のstrへ (sqliteはdatetime非対応)
        return python_value.isoformat(" ")  # datetime.datetime(2024,1,1) -> "2024-01-01 00:00:00"
    elif isinstance(
        python_value, datetime.date
    ):  # dateはdatetimeのサブクラス注意 date型の場合はISO 8601のstrへ (sqliteはdate非対応)
        return python_value.isoformat()  # datetime.date(2024,1,1) -> "2024-01-01"
    elif isinstance(python_value, Path):
        return python_value.__str__()
    elif isinstance(python_value, bytes):
        return sqlite3.Binary(python_value)
    elif isinstance(python_value, sqlite3.Binary):  # sqlite EQ前(Fieldで定義された場合は2度目以降こっちを通る)
        return python_value
    elif python_value is None:
        return None
    else:
        raise sql_module.exceptions.SQLTypeError
