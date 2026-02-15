# 標準ライブラリ
from dataclasses import dataclass
import sqlite3
from typing import Literal
from pathlib import Path
import datetime

from sql_module.sqlite.table.column.name import ColumnName
from sql_module.sqlite.table.column.column_constraint import ColumnConstraint
from sql_module import Query


class SubQuery(Query):
    pass
