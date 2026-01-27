# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass, field
import datetime

# 主要要素
from sql_module.sqlite.driver import Driver
from sql_module.sqlite.table.name import TableName
from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column import Column
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint


@dataclass
class ColumnInfo:
    name: str
    type: str
    not_null: bool
    default_value: str
    primary_key: bool
    unique: bool = False
    references: str | None = None
    index: bool = False
    message: list[str] = field(default_factory=list)


@dataclass
class IndexCounter:
    unique_index: int = 0  # ユニークで最初のカラム
    not_unique_index: int = 0  # インデックスで最初のカラム
    multi_unique_first_index: int = 0  # 複合ユニークで最初のカラム
    multi_not_unique_first_index: int = 0  # 複合インデックスで最初のカラム

    def add(self, is_unique: bool, is_multi: bool):
        if is_unique:
            if is_multi:
                self.multi_unique_first_index += 1
                return
            self.unique_index += 1
            return
        if is_multi:
            self.multi_not_unique_first_index += 1
            return
        self.not_unique_index += 1

    @property
    def message(self) -> list[str] | None:
        message_list = []
        # 1つ以下なら普通のユニーク
        if (
            self.unique_index
            + self.not_unique_index
            + self.multi_unique_first_index
            + self.multi_not_unique_first_index
            < 2
        ):
            return None
        # 単体インデックスが2つ以上入っていたら
        if self.unique_index + self.not_unique_index >= 2:
            message_list.append("単体インデックスが2つ以上含まれています。なので1つのみに絞ってみましょう。")
        # 2つ以上のうち、複合インデックスが1つ以上入っていたら
        elif self.multi_unique_first_index + self.multi_not_unique_first_index >= 1:
            message_list.append(
                "単体インデックスや複合の最初のカラムを含めたインデックスが2つ以上含まれています。複合インデックスの最初のカラムは実質単体インデックスと同じですのでなるべく1つのみに絞ってみましょう。"
            )
        return message_list

    @property
    def choice(self) -> str:
        """このカラムのインデックスを代表するなら...？ってやつ"""
        if self.unique_index > 0:
            return "unique"
        if self.not_unique_index > 0:
            return "index"
        # 何もインデックスがない場合。あと複合インデックスとかは後にする。
        return None


@dataclass
class MultiIndex:
    column_list: list
    unique: bool


@dataclass
class Info:
    driver: Driver
    table_name: TableName
    column_info_list: list[ColumnInfo] = field(default_factory=list)
    multi_index_list: list[MultiIndex] = field(default_factory=list)

    def __post_init__(self):
        """必要な情報を取得"""
        self.raw_column_list = self.get_raw_column_list()
        self.raw_foreign_key_list = self.get_raw_foreign_key_list()
        self.raw_index_list = self.get_raw_index_list()

    def __repr__(self):
        text = "column_info_list:\n"
        for column_info in self.column_info_list:
            text += f"{column_info}\n"
        text += "multi_index_list\n"
        for multi_index in self.multi_index_list:
            text += f"{multi_index}\n"
        return text

    def get_raw_column_list(self) -> list[dict[str]]:
        self.driver.execute(f"PRAGMA table_info({self.table_name.now});")
        return self.driver.fetchall(True)

    def get_raw_foreign_key_list(self) -> list[dict[str]]:
        self.driver.execute(f"PRAGMA foreign_key_list({self.table_name.now});")
        return self.driver.fetchall(True)

    def get_raw_index_list(self) -> list[dict[str]]:
        self.driver.execute(f"PRAGMA index_list({self.table_name.now});")
        return self.driver.fetchall(True)

    def get_index(self, index_name: str) -> list[dict[str]]:
        self.driver.execute(f"PRAGMA index_info({index_name});")
        return self.driver.fetchall(True)

    def get_foreign_key(self, column_name: str) -> str | None:
        """必要な情報を取得"""
        raw_column_foreign_key_list = [
            raw_foreign_key for raw_foreign_key in self.raw_foreign_key_list if raw_foreign_key["from"] == column_name
        ]
        # 外部キーがあったらそれ
        if raw_column_foreign_key_list.__len__() > 0:
            raw_column_foreign_key = raw_column_foreign_key_list[0]
            foreign_key_table_name = raw_column_foreign_key["table"]
            foreign_key_column_name = raw_column_foreign_key["to"]
            return f"{foreign_key_table_name}.{foreign_key_column_name}"

        return None

    def get_index_counter(self, column_name: str):
        index_counter = IndexCounter()
        for raw_index in self.raw_index_list:
            is_multi = False

            index_name = raw_index["name"]
            seq_index_list = self.get_index(index_name)

            if seq_index_list.__len__() > 1:
                is_multi = True

            seq_index = seq_index_list[0]
            index_column_name = seq_index["name"]
            if column_name == index_column_name:
                is_unique = bool(raw_index["unique"])
                index_counter.add(is_unique, is_multi)

        return index_counter

    def get_multi_index_list(self) -> list[MultiIndex]:
        multi_index_list = []
        for raw_index in self.raw_index_list:
            index_name = raw_index["name"]
            seq_index_list = self.get_index(index_name)

            if seq_index_list.__len__() == 1:
                continue

            column_list = [seq_index["name"] for seq_index in seq_index_list]
            is_unique = bool(raw_index["unique"])
            multi_index = MultiIndex(column_list=column_list, unique=is_unique)
            multi_index_list.append(multi_index)

        return multi_index_list

    def set_info(self):
        column_info_list = []
        for raw_column in self.raw_column_list:
            column_info = ColumnInfo(
                name=raw_column["name"],
                type=raw_column["type"],
                not_null=bool(raw_column["notnull"]),
                default_value=raw_column["dflt_value"],
                primary_key=bool(raw_column["pk"]),
            )
            column_name = raw_column["name"]

            raw_column_foreign_key = self.get_foreign_key(column_name)
            if raw_column_foreign_key:
                column_info.references = raw_column_foreign_key

            # ここからidx
            index_counter = self.get_index_counter(column_name)

            if index_counter.choice == "unique":
                column_info.unique = True
                column_info.index = True
            if index_counter.choice == "index":
                column_info.index = True

            # メッセージ
            if not index_counter.message is None:
                column_info.message += index_counter.message

            column_info_list.append(column_info)

        multi_index_list = self.get_multi_index_list()

        self.column_info_list = column_info_list
        self.multi_index_list = multi_index_list
