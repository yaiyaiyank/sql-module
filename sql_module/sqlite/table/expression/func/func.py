"""集計関数"""

from sql_module import Query, Column, expressions


class Func(Query):
    pass


class FuncAll(Func, expressions.Expression):
    """戻り値が複数行のやつ"""

    pass


class FuncOne(Func, expressions.ScalarzationExpression):
    """集計関数"""


class Count(FuncOne):
    def __init__(self, column: Column | None = None):
        query = Query()
        if isinstance(column, Column):
            query += f"COUNT({column.name})"
        if column is None:
            query += "COUNT(*)"
        self.straight_set(query)


class Sum(FuncOne):
    def __init__(self, column: Column):
        query = Query()
        query += f"SUM({column.name})"
        self.straight_set(query)


class Average(FuncOne):
    def __init__(self, column: Column):
        query = Query()
        query += f"AVG({column.name})"
        self.straight_set(query)


class Min(FuncOne):
    def __init__(self, column: Column):
        query = Query()
        query += f"MIN({column.name})"
        self.straight_set(query)


class Max(FuncOne):
    def __init__(self, column: Column):
        query = Query()
        query += f"MAX({column.name})"
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
    def __init__(self, column: Column):
        query = Query()
        query += f"LENGTH({column.name})"
        self.straight_set(query)


class Lower(FuncAll):
    def __init__(self, column: Column):
        query = Query()
        query += f"LOWER({column.name})"
        self.straight_set(query)


class Upper(FuncAll):
    def __init__(self, column: Column):
        query = Query()
        query += f"UPPER({column.name})"
        self.straight_set(query)


class Left(FuncAll):
    def __init__(self, column: Column, n: int):
        query = Query()
        query += f"substr({column.name}, 1, {n})"
        self.straight_set(query)


class Right(FuncAll):
    def __init__(self, column: Column, n: int):
        query = Query()
        query += f"substr({column.name}, -{n})"
        self.straight_set(query)
