<h1 align="center">SQLを、オブシコで。</h1>

# 基本データ型の拡張モジュール

自分のエコシステム用につくったSQLのユーティリティ的なモジュール<br>
役立ちそう ∧ 機密情報なし なのでパブリックで公開

# install
### 動作環境
* Python 3.13↑
### インストール方法 
uvなら
```bash
uv add git+https://github.com/yaiyaiyank/sql-module
```
pipなら
```bash
pip install git+https://github.com/yaiyaiyank/sql-module
```


使用例:
```python
# SQLiteDataBaseオブジェクトを定義
db_path = r"C:\aaaaaaaa.db"
sqlite_database = sql_module.SQLiteDataBase(db_path) # 引数db_pathは文字列, pathlib.Pathに対応しています。
# ちなみに、SQLiteDataBaseに何も入れなければインメモリデータベースになります。
# database = sql_module.SQLiteDataBase()
```
テーブルやカラムなどをオブジェクト指向で操作することが可能
```python
# テーブルのフレームワーク: TableDefinitionを用いてテーブル定義
class ScreenName(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        # unique
        self.screen_name_column = self.get_column("screen_name", str, not_null=True, unique=True)

screen_name: ScreenName = sqlite_database.get_table_definition(ScreenName)

# 一応、↓の方法でTableやColumnのオブジェクト定義もできる

# Tableオブジェクトを作成
# screen_name_table = database.get_table("screen_name")
# Columnオブジェクトを作成
# id_column = screen_name_table.get_column("id", type=int, primary=True)
# screen_name_column = screen_name_table.get_column("screen_name", str, not_null=True, unique=True)
# created_at_column = screen_name_table.get_column("created_at", type=datetime.datetime, default_value="CURRENT_TIMESTAMP")
# updated_at_column = screen_name_table.get_column("updated_at", type=datetime.datetime, default_value="CURRENT_TIMESTAMP")

# ただ、TableDefinitionを用いれば、IDTableDefinitionならidカラム、さらにAtIDTableDefinitionならcreated_at, updated_atの定義が自動でされ、update時にupdated_atの入力も対応しているため、基本的にAtIDTableDefinitionのフレームワークを使うほうがいい
```
他のテーブル定義
```python

# ユーザー
class User(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        # unique
        # 命名規則: rest_id_columnと書く時、それはrest.idにreferencesしてるとき。今回はrest_idそのものなのでアンダーバーをつける。
        # ChatGPTいわく、Xのrest_idは巨大整数問題を避けるため 文字列で返す。intでない。
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
        )  # ユーザー名が使われなくなって30日くらいしてから経ってから他のrest_idがそのユーザー名を使用し始める可能性がある

user_screen_name: UserScreenName = sqlite_database.get_table_definition(UserScreenName)
# 複合ユニーク
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
        self.already_download_column = self.get_column("already_download", bool, default_value=False)
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
```
### Create
```python
# テーブル作成
user.create()
screen_name.create()
user_screen_name.create(user_screen_uni)
post.create()
# インデックス作成
user_screen_name.make_index(user_screen_name.screen_name_id_column, user_screen_name.user_id_column)
```
```python
# 戻り値としてQueryオブジェクトがあり、createメソッドのis_executeをFalseにして取得したCreateオブジェクトを実行するやり方も可能。これはInsert, Update, Selectも同様。
create = user.create(is_execute=False)
create.execute()
```
```python
# Query.measurementでそのQueryオブジェクトが持っている文字・プレースホルダの情報を見ることができる。
create = user.create(is_execute=False)
create.measurement()
```
### Insert
```python
# ユーザー名を登録
screen_name_ = "hogehogepiya~"
screen_name_record = [sql_module.Field(screen_name.screen_name_column, screen_name_, upsert=True)]
insert = screen_name.insert(screen_name_record, is_returning_id=True)
screen_name_id = insert.fetch_id()
```
```python
# ユーザー情報を登録
user_record = [
    sql_module.Field(user._rest_id_column, 114514114514, upsert=True),
    sql_module.Field(user.current_name_column, "ユ"),
    sql_module.Field(user.current_screen_name_id_column, screen_name_id),
    sql_module.Field(user.description_column, "こむにちょま！"),
    sql_module.Field(user.follows_count_column, 1),
    sql_module.Field(user.followers_count_column, 10),
]
insert = user.insert(user_record, is_returning_id=True)
user_id = insert.fetch_id()
```
```python
# ユーザー名・ユーザー情報の中間テーブルを登録
user_screen_name_record = [
    sql_module.Field(user_screen_name.user_id_column, user_id, upsert=True),
    sql_module.Field(user_screen_name.screen_name_id_column, screen_name_id, upsert=True),
]
insert = user_screen_name.insert(user_screen_name_record, is_returning_id=True)
```
```python
# ポストを登録
user_screen_name_record = [
    sql_module.Field(user_screen_name.user_id_column, user_id, upsert=True),
    sql_module.Field(user_screen_name.screen_name_id_column, screen_name_id, upsert=True),
]
insert = user_screen_name.insert(user_screen_name_record, is_returning_id=True)
```
```python
success_post_id_list = []
post_record = [
    sql_module.Field(post._post_id_column, 8101919364364, upsert=True),
    sql_module.Field(post.user_id_column, user_id),
    sql_module.Field(post.content_column, "うなっ😳\n\nまたこんど！"),
    sql_module.Field(post.date_column, datetime.datetime(2026, 1, 12)), # タイムゾーン注意
    # already_download_columnは今じゃない
    sql_module.Field(post.favorite_count_column, 3),
    sql_module.Field(post.quote_count_column, 2),
    sql_module.Field(post.reply_count_column, 1),
    sql_module.Field(post.retweet_count_column, 1),
    sql_module.Field(post.bookmark_count_column, 2),
    sql_module.Field(post.view_count_column, 30),
    sql_module.Field(post.source_column, "Twitter of Windroid"),
    sql_module.Field(post.lang_column, "jp"),
]
insert = post.insert(post_record, is_returning_id=True)
success_post_id_list.append(insert.fetch_id())
```
### Update
```python
# データベース登録に成功したポストIDを登録
for post_id in post_id_list:
    post_record = sql_module.Field(post.already_download_column, True)
    where = sql_module.wheres.Eq(post.id_column, post_id)
    post.update(post_record, where)
```
### Select
#### いちばん簡単なselect
```python
# SELECT * FROM post
select = post.select()
select.fetchall() # デフォルトではdict機能とtupleが合わさったsqlite3.Rowとして取得。[sqlite3.Row, sqlite3.Row, ..., sqlite3.Row]みたいな表示になっているので、開発時は適さない。
```
```python
select = post.select()
select.fetchall(dict_output=True) # dictとして取得。表示そのままの値が見れるため、開発時では専らこっちを使う。
```
```python
select = post.select()
select.fetchmany(3) # 3行だけfetch
select.fetchmany(2) # 追加で2行だけfetch
```
```python
select = post.select()
select.fetchone() # 1行だけfetch
```
#### limit
```python
# SELECT * FROM post      LIMIT 8
select = post.select(limit=8)
select.fetchall(dict_output=True)
```
#### 式にカラム指定 (SELECTとFROMの間のことを「式」と呼んでいます。)
```python
# SELECT post.content FROM post
select = post.select(post.content_column)
select.fetchall(dict_output=True)
```
```python
select = post.select(post.content_column)
select.fetchall_value_list() # 単一カラムの値のリストが欲しい場合
```
```python
select = post.select(post.content_column)
select.fetchmany_value_list(2) # 単一カラムの値のリストを2つまでが欲しい場合
```
```python
# SELECT post.id, post._post_id FROM post
select = post.select([post.id_column, post._post_id_column])
select.fetchall(dict_output=True)
```
#### where条件を指定
```python
# SELECT post.content FROM post  WHERE post.date >= :p0, {'p0': '2025-01-02 00:00:00'}
where = sql_module.conds.GreaterEq(post.date_column, datetime.date(2025, 1, 2))
select = post.select(post.content_column, where)
select.fetchall(dict_output=True)
```
```python
# SELECT post.user_id FROM post  WHERE post.date >= :p0 AND post.date < :p1, {'p0': '2025-01-02 00:00:00', 'p1': '2026-02-12 00:00:00'}
where = sql_module.conds.Range(post.date_column, datetime.date(2025, 1, 2), datetime.date(2026, 2, 12))
select = post.select(1, where)
select.fetchall(dict_output=True)
```
```python
# SELECT post.user_id FROM post  WHERE post.content LIKE '%' || :p0 || '%', {'p0': 'ヤイヤイ'}
where = sql_module.conds.Contains(post.content_column, "ヤイヤイ")
select = post.select(post.user_id_column, where)
select.fetchall(dict_output=True)
```
```python
# SELECT post.content FROM post  WHERE post.id IN (:p0, :p1, :p2), {'p0': 1, 'p1': 2, 'p2': 4}
where = sql_module.conds.In(post.id_column, [1, 2, 4])
select = post.select(post.content_column, where)
select.fetchall(dict_output=True)
```
```python
# SELECT post.id, post.date FROM post  WHERE post.content = :p0 AND post.date >= :p1, {'p0': '', 'p1': '2025-02-04 12:00:00'}
where = sql_module.conds.Eq(post.content_column, "") & sql_module.conds.GreaterEq(post.date_column, datetime.datetime(2025, 2, 4, 12, 0, 0))
select = post.select([post.id_column, post.date_column], where)
select.fetchall(dict_output=True)
```
#### 式にcond, func
```python
# SELECT post.id >= :p0 FROM post, {'p0': 10}
select = post.select(sql_module.conds.GreaterEq(post.id_column, 10))
select.fetchall(dict_output=True)
```
```python
# SELECT UPPER(post.content) FROM post
select = post.select(sql_module.funcs.Upper(post.content_column))
select.fetchall(dict_output=True)
```
```python
# SELECT COUNT(*) FROM post
select = post.select(sql_module.funcs.Count())
select.fetchone_value()
```
```python
# SELECT COUNT(post.content) FROM post
select = post.select(sql_module.funcs.Count(post.content_column))
select.fetchone_value()
```
```python
# SELECT AVG(post.retweet_count) FROM post
select = post.select(sql_module.funcs.Count())
select.fetchone_value()
```
```python
# SELECT EXISTS (SELECT 1 FROM post  WHERE post.date >= :p0 AND post.date < :p1    ), {'p0': '2026-02-11 00:00:00', 'p1': '2026-02-16 00:00:00'}
select_pre = post.select(1, sql_module.conds.Range(post.date_column, datetime.date(2026, 2, 11), datetime.date.today()))
exists = sql_module.conds.Exists(select_pre)
select = post.select(exists, is_from=False)
select.fetchone_value()
```
#### join
```python
# SELECT user.current_name FROM post JOIN user ON user.id = post.user_id
join = sql_module.Join(post.user_id_column)
select = post.select(user.current_name_column, join=join)
select.fetchall(dict_output=True)
```
```python
# 実践的なクエリ: screen_name(@の後ろの文字列)が'otomachiuna'の全てのuser_idのpost.post_idを取得
# 補足: そのscreen_nameの持ち主がアカウントを削除し、その30日後以降に別のuser_idのアカウントがそのscreen_nameを取得するなどしてuser_idが増える
# SELECT post.id FROM post JOIN screen_name ON screen_name.id = user_screen_name.screen_name_id JOIN user_screen_name ON user_screen_name.user_id = post.user_id WHERE screen_name.screen_name = :p0, {'p0': 'otomachiuna'}
target_screen_name = "otomachiuna"
join = [sql_module.Join(user_screen_name.screen_name_id_column), sql_module.Join(post.user_id_column, target_column=user_screen_name.user_id_column)]
where = sql_module.conds.Eq(screen_name.screen_name_column, target_screen_name)
select = post.select(post.id_column, join=join, where=where)
select.fetchall(dict_output=True)
```
#### group_by
```python
# 実践的なクエリ: いいね数の統計データみたいなのを取るのに使う。
# SELECT COUNT(post.favorite_count) FROM post   GROUP BY post.favorite_count
select = post.select(sql_module.funcs.Count(post.favorite_count_column), group_by=post.favorite_count_column)
select.fetchall(dict_output=True)
```
#### order_by
```python
# 実践的なクエリ: ページングや無限スクロールで使える、日付降順(+境界で同じ日付が出てくる可能性のためにユニークなid降順)でlimitづつ取得
# SELECT * FROM post  WHERE post.date < :p0 OR (post.date = :p1 AND post.id < :p2)   ORDER BY post.date DESC, post.id DESC LIMIT 8, {'p0': '2100-01-01 00:00:00', 'p1': '2100-01-01 00:00:00', 'p2': 10000000000}
# SELECT * FROM post  WHERE post.date < :p0 OR (post.date = :p1 AND post.id < :p2)   ORDER BY post.date DESC, post.id DESC LIMIT 8, {'p0': '2026-01-12 00:00:00', 'p1': '2026-01-12 00:00:00', 'p2': 19}
# SELECT * FROM post  WHERE post.date < :p0 OR (post.date = :p1 AND post.id < :p2)   ORDER BY post.date DESC, post.id DESC LIMIT 8, {'p0': '2024-03-15 10:07:29', 'p1': '2024-03-15 10:07:29', 'p2': 15}
last_date = datetime.date(2100, 1, 1)
last_id = 10**10
limit = 8
def get_seek_page(post, last_date: str, last_id: int):
    
    where = sql_module.conds.Less(post.date_column, last_date) | sql_module.conds.Eq(post.date_column, last_date) & sql_module.conds.Less(post.id_column, last_id)
    order_by = [sql_module.OrderBy(post.date_column, False), sql_module.OrderBy(post.id_column, False)]

    select = post.select(None, where, order_by=order_by, limit=8)
    fetchall = select.fetchall(dict_output=True)
    return fetchall, fetchall[-1]["date"], fetchall[-1]["id"] # out of rangeするまでできると思う
fetchall1, last_date, last_id = get_seek_page(post, last_date, last_id)
fetchall2, last_date, last_id = get_seek_page(post, last_date, last_id)
fetchall3, last_date, last_id = get_seek_page(post, last_date, last_id)
```
### tips

1. AUTO_INCREMENTのidがオーバーフローするには1日100万回レコード追加したとしても20万年かかるのでその心配はない by ChatGPT<br><br>
追記: ChatGPTいわく「**PRIMARY KEY AUTOINCREMENT は「一度使った値を二度と再利用しない」「常に過去最大値より大きい値」という追加ルールがつき、内部の sqlite_sequence を使います。速度や断片化の面で不利なことが多いので、厳密な単調増加が必要な場合以外は付けないのが定石です。**」とのことなので、AUTO_INCREMENTは廃止しました。ChatGPTの意見ひとつで設計を変えるのがこの私です。


