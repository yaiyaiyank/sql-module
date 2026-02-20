"""集計関数"""

from sql_module import Query, expressions
from sql_module.sqlite.table.column.interface import ColumnLike


class Func(Query):
    """関数"""

    pass


class FuncAll(Func, expressions.Expression):
    """戻り値が複数行のやつ"""

    pass


class FuncOne(Func, expressions.ScalarzationExpression):
    """集計関数"""


class Count(FuncOne):
    def __init__(self, column: ColumnLike | None = None, is_column_only_name: bool = False):
        query = Query()
        if isinstance(column, ColumnLike):
            query += "COUNT(" + column.name.to_query(is_column_only_name) + ")"
        if column is None:
            query += "COUNT(*)"
        self.straight_set(query)


class Sum(FuncOne):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "SUM(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Average(FuncOne):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "AVG(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Min(FuncOne):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "MIN(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Max(FuncOne):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "MAX(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Coalesce(FuncOne):
    def __init__(self, func: Sum | Average | Min | Max, default_value: int = 0):
        """COALESCE(SUM(amount), 0)みたいな"""
        query = Query()
        query += "COALESCE("
        query += func
        query += f", {default_value})"
        self.straight_set(query)


class Length(FuncAll):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "LENGTH(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Lower(FuncAll):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "LOWER(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Upper(FuncAll):
    def __init__(self, column: ColumnLike, is_column_only_name: bool = False):
        query = Query()
        query += "UPPER(" + column.name.to_query(is_column_only_name) + ")"
        self.straight_set(query)


class Left(FuncAll):
    def __init__(self, column: ColumnLike, n: int, is_column_only_name: bool = False):
        query = Query()
        query += "substr(" + column.name.to_query(is_column_only_name) + f", 1, {n})"
        self.straight_set(query)


class Right(FuncAll):
    def __init__(self, column: ColumnLike, n: int, is_column_only_name: bool = False):
        query = Query()
        query += "substr(" + column.name.to_query(is_column_only_name) + f", -{n})"
        self.straight_set(query)
