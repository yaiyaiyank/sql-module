from dataclasses import dataclass
from sql_module.sqlite.table.name import TableName


@dataclass
class ColumnName:  # columnオブジェクトに持たせてシングルトン的な感じで使うイミュータブルオブジェクト
    table_name: TableName
    now: str
    old: str | None = None

    def set_new_name(self, name: str):
        if not self.old is None:
            raise ValueError(f"set_new_nameメソッドはoldがNoneのときのみです。old: {self.old}")
        if not isinstance(name, str):
            raise ValueError(f"nameはstrのみ。name: {type(name)}")
        self.old = self.now
        self.now = name

    def return_name(self):
        if self.old is None:
            raise ValueError("return_nameメソッドはoldがstrのときのみです。")
        self.now = self.old
        self.old = None

    def get(self, is_as: bool = False):
        """
        is_as (bool): ASを含めるかどうか
        fromなどで使う
        """
        # is_asするなら
        if not self.old is None and is_as:
            return f"{self.table_name.now}.{self.old} AS {self.now}"
        # ここでis_asしないならその前でされていて、table名は不要なので
        if not self.old is None and not is_as:
            return self.now
        # 通常
        return f"{self.table_name.now}.{self.now}"
