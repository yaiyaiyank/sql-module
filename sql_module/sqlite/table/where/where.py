# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime
from typing import Self

from sql_module import Column, Query, query_join_comma


class Where(Query):
    def __and__(self, other: Self) -> Self:
        return And(self, other)

    def __or__(self, other: Self) -> Self:
        return Or(self, other)

    def __invert__(self):
        return Not(self)


class WhereBool(Where):
    pass


class WhereCond(Where):
    pass


class And(WhereBool):
    def __init__(self, left_where: Where, right_where: Where):
        if not (isinstance(left_where, Where) and isinstance(right_where, Where)):
            raise TypeError(
                f"演算: '&'はWhereオブジェクト同士でのみ行えます。'{left_where.__class__.__name__} & {right_where.__class__.__name__}'はできません。"
            )
        # Orオブジェクトは演算の順序でアレ
        if isinstance(left_where, Or):
            left_where = "(" + left_where + ")"
        if isinstance(right_where, Or):
            right_where = "(" + right_where + ")"
        query = left_where + " AND " + right_where

        self.straight_set(query)


class Or(WhereBool):
    def __init__(self, left_where: Where, right_where: Where):
        if not (isinstance(left_where, Where) and isinstance(right_where, Where)):
            raise TypeError(
                f"演算: '|'はWhereオブジェクト同士でのみ行えます。'{left_where.__class__.__name__} | {right_where.__class__.__name__}'はできません。"
            )
        # Andオブジェクトは演算の順序でアレ
        if isinstance(left_where, And | Range):  # RangeもAnd系
            left_where = "(" + left_where + ")"
        if isinstance(right_where, And | Range):
            right_where = "(" + right_where + ")"
        query = left_where + " OR " + right_where

        self.straight_set(query)


class Not(WhereBool):
    def __init__(self, where: Where):
        if not isinstance(where, Where):
            raise TypeError(f"演算: '~'はWhereオブジェクトでのみ行えます。'~{where.__class__.__name__}'はできません。")

        query = "NOT (" + where + ")"
        self.straight_set(query)


class Eq(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | None):
        """
        例:
        'user.id = :p0', {'p0': 2}
        'user.created_at >= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            query += f"{column.name.get()} IS NULL"
            self.straight_set(query)

        query += f"{column.name.get()} = "
        # カラムなら列比較
        if isinstance(value, Column):
            query += value.name.get()
        # 値
        else:
            query *= column.constraint.get_sql_value(value)

        self.straight_set(query)


class GreaterEq(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column):
        """
        例:
        'user.age >= :p0', {'p0': 20}
        'user.created_at >= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name.get()} >= "
        # カラムなら列比較
        if isinstance(value, Column):
            query += value.name.get()
        # 値
        else:
            query *= column.constraint.get_sql_value(value)

        self.straight_set(query)


class Greater(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column):
        """
        例:
        'user.age > :p0', {'p0': 20}
        'user.created_at > :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name.get()} > "
        # カラムなら列比較
        if isinstance(value, Column):
            query += value.name.get()
        # 値
        else:
            query *= column.constraint.get_sql_value(value)

        self.straight_set(query)


class LessEq(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column):
        """
        例:
        'user.age <= :p0', (20,)
        'user.created_at <= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name.get()} <= "
        # カラムなら列比較
        if isinstance(value, Column):
            query += value.name.get()
        # 値
        else:
            query *= column.constraint.get_sql_value(value)

        self.straight_set(query)


class Less(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column):
        """
        例:
        'user.age < :p0', (20,)
        'user.created_at < :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name.get()} < "
        # カラムなら列比較
        if isinstance(value, Column):
            query += value.name.get()
        # 値
        else:
            query *= column.constraint.get_sql_value(value)

        self.straight_set(query)


class StartsWith(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date):
        """
        例:
        'device.bender LIKE :p0%', {'p0': 'Xiaomi'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")
        else:
            query += f"{column.name.get()} LIKE "
            query *= column.constraint.get_sql_value(value)
            query += " || '%'"

        self.straight_set(query)


class EndsWith(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date):
        """
        例:
        'device.bender LIKE %:p0', {'p0': 'ホールディングス'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")
        else:
            query += f"{column.name.get()} LIKE '%' || "
            query *= column.constraint.get_sql_value(value)

        self.straight_set(query)


class Contains(WhereCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date):
        """
        例:
        'device.bender LIKE %:p0', {'p0': 'うおｗ'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")
        else:
            query += f"{column.name.get()} LIKE '%' || "
            query *= column.constraint.get_sql_value(value)
            query += " || '%'"

        self.straight_set(query)


class In(WhereCond):
    def __init__(self, column: Column, value_list: list[str | int | bytes | Path | datetime.date | None]):
        """
        例:
        'channel.name IN (:p0, :p1, :p2)', {'p0': 'おお', 'p0': 'どわーｗふ', 'p0': 'ぬ'}
        """
        query = Query()

        query += f"{column.name.get()} IN ("
        value_query_list = [Query() * column.constraint.get_sql_value(value_) for value_ in value_list]
        joined_value_query = query_join_comma(value_query_list)
        query += joined_value_query
        query += ")"

        self.straight_set(query)


class Range(WhereCond):
    def __init__(
        self,
        column: Column,
        start_value: str | int | bytes | Path | datetime.date,
        end_value: str | int | bytes | Path | datetime.date,
        include_start: bool = True,
        include_end: bool = False,
    ):
        """
        例:
        'user.updated_at >= :p0 AND user.updated_at < :p1', {'p0': '2022-02-22 00:00:00', 'p1': '2022-02-23 00:00:00'}
        """
        if start_value is None or end_value is None:
            raise TypeError("None禁止")

        if include_start:
            greater = GreaterEq(column, start_value)
        else:
            greater = Greater(column, start_value)
        if include_end:
            less = LessEq(column, end_value)
        else:
            less = Less(column, end_value)

        self.straight_set(greater & less)


class TRUE(WhereCond):
    def __init__(self):
        """単位元: true"""
        query = Query("1 = 1")

        self.straight_set(query)


class FALSE(WhereCond):
    def __init__(self):
        """単位元: false"""
        query = Query("0 = 1")

        self.straight_set(query)
