<div style="text-align:center; font-size:60px;"><b>SQLを、オブシコで。</b></div>

## 目的

みなさんこんばんは、にじきんじ所属のヤイさんです。このリポジトレィは肥大化した自作ライブラリをポリレポ化する試みのsql部分です。
ただリファクタする以外に機能改善をちゃんとやっているのでねって感じです。

## 依存関係

トップモジュール: yaiで管理された[Logモジュール](https://github.com/yaiyaiyank/logging_module)とこのモジュール自体に依存している

## usage (今は自分以外使えないです...申し訳なさすぎて、さすがの自分も横転)

テーブルやカラムなどをオブジェクト指向で操作することが可能だ
```python
from yai.entry.sql_module import sqlite

# sqlite.SQLiteDataBaseオブジェクトを定義
db_path = r"C:\aaaaaaaa.db"
database = sqlite.SQLiteDataBase(db_path) # 引数db_pathは文字列, pathlib.Pathに対応しています。

# SQLiteDataBaseに何も入れなければインメモリデータベースになります。
# database = sqlite.SQLiteDataBase()

# sqlite.SQLiteDataBaseオブジェクトからsqlite.Tableオブジェクトを定義 
table = database.get_table("test_table")

# sqlite.Tableオブジェクトからsqlite.Columnオブジェクトを定義 
column1 = table.get_column("uow", int)
column2 = table.get_column("oo", str)

```
### Create
```python
table.create([column1, column2])
```
戻り値としてCreateオブジェクトがあり、createメソッドのis_executeをFalseにしてCreateオブジェクトを取得して実行するやり方も可能。
```python
create = table.create([column1, column2], is_execute=False)
create.execute()
```

<!-- ここからスマホでの編集で、プレビューなしです。あとでPCでimportのパスとか確認するです。 -->

カラムを属性として持ち、カラム名定義からcreateメソッドまでをサポートするフレームワークを提供しています。
```python
from yai.entry.sql_module import sqlite



class WorkTable(sqlite.DateIDTableDifinition):
    def set_column(self)
```





### tips

1. AUTO_INCREMENTのidがオーバーフローするには1日100万回レコード追加したとしても20万年かかるのでその心配はない by ChatGPT
(追記: ChatGPTいわく「**PRIMARY KEY AUTOINCREMENT は「一度使った値を二度と再利用しない」「常に過去最大値より大きい値」という追加ルールがつき、内部の sqlite_sequence を使います。速度や断片化の面で不利なことが多いので、厳密な単調増加が必要な場合以外は付けないのが定石です。**」とのことなので、AUTO_INCREMENTは廃止しました。ChatGPTの意見ひとつで設計を変えるのがこの私です。)

