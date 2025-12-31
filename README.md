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

テーブルやカラムなどをオブジェクト指向で操作することが可能だ
```python
import sql_module

# SQLiteDataBaseオブジェクトを定義
db_path = r"C:\aaaaaaaa.db"
database = sql_module.SQLiteDataBase(db_path) # 引数db_pathは文字列, pathlib.Pathに対応しています。

# SQLiteDataBaseに何も入れなければインメモリデータベースになります。
# database = sql_module.SQLiteDataBase()

# SQLiteDataBaseオブジェクトからTableオブジェクトを定義 
work_table = database.get_table("work")

# TableオブジェクトからColumnオブジェクトを定義 
name_column = work_table.get_column("name", str)
user_id_column = work_table.get_column("user_id", int)

```
### Create
```python
work_table.create([name_column, user_id_column])
```
戻り値としてCreateオブジェクトがあり、createメソッドのis_executeをFalseにしてCreateオブジェクトを取得して実行するやり方も可能。
```python
create = work_table.create([name_column, user_id_column], is_execute=False)
create.execute()
```

# テーブルフレームワーク
カラムを属性として持ち、カラム名定義からcreateメソッドまでをサポートするフレームワークを提供しています。
```python
import sql_module

class Work(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        # Columnオブジェクトを定義
        self.name_column = self.table.get_column("name", str)
        self.user_id_column = self.table.get_column("user_id", int)

database = sql_module.SQLiteDataBase()
work = database.get_table_definition("work", Work)
work.create()

# work.name_columnとしてnameカラムにアクセスできたりします。便利！

```





### tips

1. AUTO_INCREMENTのidがオーバーフローするには1日100万回レコード追加したとしても20万年かかるのでその心配はない by ChatGPT<br><br>
追記: ChatGPTいわく「**PRIMARY KEY AUTOINCREMENT は「一度使った値を二度と再利用しない」「常に過去最大値より大きい値」という追加ルールがつき、内部の sqlite_sequence を使います。速度や断片化の面で不利なことが多いので、厳密な単調増加が必要な場合以外は付けないのが定石です。**」とのことなので、AUTO_INCREMENTは廃止しました。ChatGPTの意見ひとつで設計を変えるのがこの私です。
