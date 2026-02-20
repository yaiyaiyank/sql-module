from dataclasses import dataclass

from sql_module import Query


@dataclass
class ColumnName:
    """
    カラム名
    例: テーブル名: post, カラム名: id の場合、
    name: id
    table_name: post
    __str__(): post.id
    """

    name: str
    table_name: str

    def __str__(self):
        return f"{self.table_name}.{self.name}"

    def to_query(self, is_column_only_name: bool = False) -> Query:
        if is_column_only_name:
            return Query(self.name)
        return Query(self.__str__())
