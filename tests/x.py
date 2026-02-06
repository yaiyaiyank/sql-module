"""xのスクレイピングに運用されているテーブル設計"""

# 標準ライブラリ
from pathlib import Path
from typing import Literal
import re

import sql_module


def get_table_definition(db_path: Path | str | None = None):
    sqlite_database = sql_module.SQLiteDataBase(db_path)

    class ScreenName(sql_module.AtIDTableDefinition):
        """@部分の名前は、フォルダーに使うので専用に作る"""

        def set_colmun_difinition(self):
            # unique
            self.screen_name_column = self.table.get_column("screen_name", str, not_null=True, unique=True)

    screen_name: ScreenName = sqlite_database.get_table_definition(ScreenName)

    # ユーザー
    class User(sql_module.AtIDTableDefinition):
        def set_colmun_difinition(self):
            # 現在の名前
            self.current_name_column = self.table.get_column("current_name", str, not_null=True)
            # 現在のscreen_name(@部分)のid
            self.current_screen_name_id_column = self.table.get_column(
                "current_screen_name_id", int, not_null=True, references=screen_name.id_column
            )
            # 命名規則: rest_id_columnと書く時、それはrest.idにreferencesしてるとき。今回はrest_idそのものなのでアンダーバーをつける。
            # 巨大整数問題を避けるため 文字列で返す。intでない。
            self._rest_id_column = self.table.get_column("rest_id", str, not_null=True, unique=True)
            self.description_column = self.table.get_column("description", str)

    user: User = sqlite_database.get_table_definition(User)

    class UserScreenName(sql_module.AtIDTableDefinition):
        """
        rest_id == '1823879767634141185', screen_name == 'oioioi525'
        ->
        rest_id == '1823879767634141185', screen_name == 'oioioi525_2'

        rest_id == '114514', screen_name == 'nyowaaaaaa'
        ->
        rest_id == '114514', screen_name == 'oioioi525'
        みたいになれば、
        '1823879767634141185' -> 'oioioi525', 'oioioi525_2'
        'oioioi525' -> '1823879767634141185', '114514'
        といった多対多の関係になりうるので、中間テーブルを作る
        """

        def set_colmun_difinition(self):
            # unique
            self.user_id_column = self.table.get_column(
                "user_id", int, not_null=True, references=user.id_column
            )  # rest_idごとにユーザー名をころころ変える人もいます
            self.screen_name_id_column = self.table.get_column(
                "screen_name_id", int, not_null=True, references=screen_name.id_column
            )  # ユーザー名が使われなくなって30日くらいしてから経ってから他のrest_idがそのユーザー名を使用し始める可能性があるため

    user_screen_name: UserScreenName = sqlite_database.get_table_definition(UserScreenName)
    user_screen_uni = sql_module.UniqueCompositeConstraint(
        user_screen_name.user_id_column, user_screen_name.screen_name_id_column
    )

    return user, screen_name, user_screen_name, user_screen_uni
