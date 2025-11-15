from dataclasses import dataclass
from abc import ABC
from sql_module.sqlite.table.column.name import ColumnName


@dataclass
class ColumnLike(ABC):
    name: ColumnName
