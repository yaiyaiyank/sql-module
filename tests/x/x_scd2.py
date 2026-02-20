"""xのスクレイピングに運用されているテーブル設計"""

# 標準ライブラリ
from pathlib import Path
import datetime
import sql_module


class ScreenName(sql_module.AtIDTableDefinition):
    """@部分の名前は、フォルダーに使うので専用に作る"""

    def set_colmun_difinition(self):
        # unique
        self.screen_name_column = self.get_column("screen_name", str, not_null=True, unique=True)


class UserSCD2(sql_module.SCD2AtIDTableDefinition):
    def set_colmun_difinition(self):
        # is_current = 1付きでunique
        self._rest_id_column = self.get_column("_rest_id", str, not_null=True)
        # 名前
        self.name_column = self.get_column("name", str, not_null=True)
        # @部分のid
        self.screen_name_id_column = self.get_column("screen_name_id", int, not_null=True)
        # bio
        self.description_column = self.get_column("description", str, not_null=True)
        # フォロー数
        self.follows_count_column = self.get_column("follows_count", int, not_null=True)
        # フォロワー数
        self.followers_count_column = self.get_column("followers_count", int, not_null=True)


def table_init(db_path: Path | str | None = None):
    # dbの祖
    sqlite_database = sql_module.SQLiteDataBase(db_path)

    # テーブル定義
    screen_name: ScreenName = sqlite_database.get_table_definition(ScreenName)

    user: UserSCD2 = sqlite_database.get_table_definition(UserSCD2)
    user.screen_name_id_column.set_foreign_key(screen_name.id_column)

    # テーブル・インデックス作成
    screen_name.create()

    user.create()

    user.create_is_current_unique_index(user._rest_id_column)

    return screen_name, user
