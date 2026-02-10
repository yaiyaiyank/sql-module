"""xのスクレイピングに運用されているテーブル設計"""

# 標準ライブラリ
from pathlib import Path
from typing import Literal
import re
import datetime
import sql_module


def get_table_definition(db_path: Path | str | None = None):
    sqlite_database = sql_module.SQLiteDataBase(db_path)

    class ScreenName(sql_module.AtIDTableDefinition):
        """@部分の名前は、フォルダーに使うので専用に作る"""

        def set_colmun_difinition(self):
            # unique
            self.screen_name_column = self.get_column("screen_name", str, not_null=True, unique=True)

    screen_name: ScreenName = sqlite_database.get_table_definition(ScreenName)

    # ユーザー
    class User(sql_module.AtIDTableDefinition):
        def set_colmun_difinition(self):
            # unique
            # 命名規則: rest_id_columnと書く時、それはrest.idにreferencesしてるとき。今回はrest_idそのものなのでアンダーバーをつける。
            # ChatGPTいわく、rest_idは巨大整数問題を避けるため 文字列で返す。intでない。
            self._rest_id_column = self.get_column("_rest_id", str, not_null=True, unique=True)
            # 現在の名前
            self.current_name_column = self.get_column("current_name", str, not_null=True)
            # 現在のscreen_name(@部分)のid
            self.current_screen_name_id_column = self.get_column(
                "current_screen_name_id", int, not_null=True, references=screen_name.id_column
            )
            # bio
            self.description_column = self.get_column("description", str)
            # フォロー数
            self.follows_count_column = self.get_column("follows_count", int)
            # フォロワー数
            self.followers_count_column = self.get_column("followers_count", int)

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
            self.user_id_column = self.get_column(
                "user_id", int, not_null=True, references=user.id_column
            )  # rest_idごとにユーザー名をころころ変える人もいます
            self.screen_name_id_column = self.get_column(
                "screen_name_id", int, not_null=True, references=screen_name.id_column
            )  # ユーザー名が使われなくなって30日くらいしてから経ってから他のrest_idがそのユーザー名を使用し始める可能性があるため

    user_screen_name: UserScreenName = sqlite_database.get_table_definition(UserScreenName)
    user_screen_uni = sql_module.UniqueCompositeConstraint(
        user_screen_name.user_id_column, user_screen_name.screen_name_id_column
    )

    class Post(sql_module.AtIDTableDefinition):
        def set_colmun_difinition(self):
            # unique
            # rest_idと違い、こちらはint
            # 命名規則により_idが自然の場合はアンダーバーをつける。
            self._post_id_column = self.get_column("_post_id", int, not_null=True, unique=True)
            # 命名規則により_idが外部キーの場合はアンダーバーをつけない。
            self.user_id_column = self.get_column("user_id", int, not_null=True, references=user.id_column)
            # 140字まで綴る文章
            self.content_column = self.get_column("content", str, not_null=True)
            # 投稿日
            self.date_column = self.get_column("date", datetime.datetime, not_null=True)
            # ダウンロード済判定に使う
            self.already_download_column = self.get_column("already_download", bool)
            # いいね(ふぁぼ)数
            self.favorite_count_column = self.get_column("favorite_count", int, not_null=True)
            # 引用リポスト
            self.quote_count_column = self.get_column("quote_count", int, not_null=True)
            # リプ(返信)数
            self.reply_count_column = self.get_column("reply_count", int, not_null=True)
            # リポスト数
            self.retweet_count_column = self.get_column("retweet_count", int, not_null=True)
            # ブックマーク数
            self.bookmark_count_column = self.get_column("bookmark_count", int, not_null=True)
            # 表示された数
            self.view_count_column = self.get_column("view_count", int, not_null=True)
            # Twitter for (アプリのタイプ)
            self.source_column = self.get_column("source", str, not_null=True)
            # lang
            self.lang_column = self.get_column("lang", str, not_null=True)

    post: Post = sqlite_database.get_table_definition(Post)

    return user, screen_name, user_screen_name, user_screen_uni, post


# -----------ユーザーのプロフ系insert-----------
def insert_screen_name(screen_name: sql_module.AtIDTableDefinition, screen_name_: str) -> int:
    screen_name_record = [sql_module.Field(screen_name.screen_name_column, screen_name_, upsert=True)]
    insert = screen_name.insert(screen_name_record, is_returning_id=True)
    screen_name_id = insert.fetch_id()
    return screen_name_id


def insert_user(
    user: sql_module.AtIDTableDefinition,
    rest_id: str,
    name: str,
    screen_name_id: int,
    description: str,
    follows_count: int,
    followers_count: int,
) -> int:
    user_record = [
        sql_module.Field(user._rest_id_column, rest_id, upsert=True),
        sql_module.Field(user.current_name_column, name),
        sql_module.Field(user.current_screen_name_id_column, screen_name_id),
        sql_module.Field(user.description_column, description),
        sql_module.Field(user.follows_count_column, follows_count),
        sql_module.Field(user.followers_count_column, followers_count),
    ]
    insert = user.insert(user_record, is_returning_id=True)
    return insert.fetch_id()


def insert_user_screen_name(user_screen_name: sql_module.AtIDTableDefinition, user_id: int, screen_name_id: int) -> int:
    user_screen_name_record = [
        sql_module.Field(user_screen_name.user_id_column, user_id, upsert=True),
        sql_module.Field(user_screen_name.screen_name_id_column, screen_name_id, upsert=True),
    ]
    insert = user_screen_name.insert(user_screen_name_record, is_returning_id=True)
    return insert.fetch_id()


# -----------ポスト系insert-----------
def insert_post(
    post: sql_module.AtIDTableDefinition,
    _post_id: int,
    user_id: int,
    content: str,
    date: datetime.datetime,
    favorite_count: int,
    quote_count: int,
    reply_count: int,
    retweet_count: int,
    bookmark_count: int,
    view_count: int,
    source: str,
    lang: str,
) -> int:
    post_record = [
        sql_module.Field(post._post_id_column, _post_id, upsert=True),
        sql_module.Field(post.user_id_column, user_id),
        sql_module.Field(post.content_column, content),
        sql_module.Field(post.date_column, date),
        # already_download_columnは今じゃない
        sql_module.Field(post.favorite_count_column, favorite_count),
        sql_module.Field(post.quote_count_column, quote_count),
        sql_module.Field(post.reply_count_column, reply_count),
        sql_module.Field(post.retweet_count_column, retweet_count),
        sql_module.Field(post.bookmark_count_column, bookmark_count),
        sql_module.Field(post.view_count_column, view_count),
        sql_module.Field(post.source_column, source),
        sql_module.Field(post.lang_column, lang),
    ]
    insert = post.insert(post_record, is_returning_id=True)
    return insert.fetch_id()


def regist_already_download(post: sql_module.AtIDTableDefinition, post_id_list: list[int]):
    for post_id in post_id_list:
        post_record = [
            sql_module.Field(post.id_column, post_id, upsert=True),
            sql_module.Field(post.already_download_column, True),
        ]
        post.insert(post_record)
