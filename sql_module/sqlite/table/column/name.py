from dataclasses import dataclass


@dataclass
class ColumnName:
    name: str
    table_name: str

    def __str__(self):
        return f"{self.table_name}.{self.name}"
