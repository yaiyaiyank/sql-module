# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime
from typing import Self

from sql_module import Column, Query, query_join_comma, expressions
from sql_module.sqlite.table.sub_query import SubQuery


class Cond(Query, expressions.Expression):
    def __and__(self, other: Self) -> Self:
        return And(self, other)

    def __or__(self, other: Self) -> Self:
        return Or(self, other)

    def __invert__(self):
        return Not(self)


class CondBool(Cond):
    pass


class CondCond(Cond):
    def get_value_query(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        query = Query()
        # カラムなら列比較
        if isinstance(value, Column):
            query += value.name.__str__()
        elif isinstance(value, SubQuery):
            query += "("
            query += value
            query += ")"
        # 値
        else:
            query *= column.constraint.get_sql_value(value)

        return query


class And(CondBool):
    def __init__(self, left_where: Cond, right_where: Cond):
        if not (isinstance(left_where, Cond) and isinstance(right_where, Cond)):
            raise TypeError(
                f"演算: '&'はCondオブジェクト同士でのみ行えます。'{left_where.__class__.__name__} & {right_where.__class__.__name__}'はできません。"
            )
        # Orオブジェクトは演算の順序でアレ
        if isinstance(left_where, Or):
            left_where = "(" + left_where + ")"
        if isinstance(right_where, Or):
            right_where = "(" + right_where + ")"
        query = left_where + " AND " + right_where

        self.straight_set(query)


class Or(CondBool):
    def __init__(self, left_where: Cond, right_where: Cond):
        if not (isinstance(left_where, Cond) and isinstance(right_where, Cond)):
            raise TypeError(
                f"演算: '|'はCondオブジェクト同士でのみ行えます。'{left_where.__class__.__name__} | {right_where.__class__.__name__}'はできません。"
            )
        # Andオブジェクトは演算の順序でアレ
        if isinstance(left_where, And | Range):  # RangeもAnd系
            left_where = "(" + left_where + ")"
        if isinstance(right_where, And | Range):
            right_where = "(" + right_where + ")"
        query = left_where + " OR " + right_where

        self.straight_set(query)


class Not(CondBool):
    def __init__(self, where: Cond):
        if not isinstance(where, Cond):
            raise TypeError(f"演算: '~'はCondオブジェクトでのみ行えます。'~{where.__class__.__name__}'はできません。")

        query = "NOT (" + where + ")"
        self.straight_set(query)


class Eq(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery | None):
        """
        例:
        'user.id = :p0', {'p0': 2}
        'user.created_at >= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            query += f"{column.name} IS NULL"
            self.straight_set(query)

        query += f"{column.name} = "
        query += self.get_value_query(column, value)

        self.straight_set(query)


class GreaterEq(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'user.age >= :p0', {'p0': 20}
        'user.created_at >= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} >= "
        query += self.get_value_query(column, value)

        self.straight_set(query)


class Greater(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'user.age > :p0', {'p0': 20}
        'user.created_at > :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} > "
        query += self.get_value_query(column, value)

        self.straight_set(query)


class LessEq(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'user.age <= :p0', (20,)
        'user.created_at <= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} <= "
        query += self.get_value_query(column, value)

        self.straight_set(query)


class Less(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'user.age < :p0', (20,)
        'user.created_at < :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} < "
        query += self.get_value_query(column, value)

        self.straight_set(query)


class StartsWith(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'device.bender LIKE :p0%', {'p0': 'Xiaomi'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} LIKE "
        query += self.get_value_query(column, value)
        query += " || '%'"

        self.straight_set(query)


class EndsWith(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'device.bender LIKE %:p0', {'p0': 'ホールディングス'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} LIKE '%' || "
        query += self.get_value_query(column, value)

        self.straight_set(query)


class Contains(CondCond):
    def __init__(self, column: Column, value: str | int | bytes | Path | datetime.date | Column | SubQuery):
        """
        例:
        'device.bender LIKE %:p0', {'p0': 'うおｗ'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += f"{column.name} LIKE '%' || "
        query += self.get_value_query(column, value)
        query += " || '%'"

        self.straight_set(query)


class In(CondCond):
    def __init__(self, column: Column, value_list: list[str | int | bytes | Path | datetime.date | None] | SubQuery):
        """
        例:
        'channel.name IN (:p0, :p1, :p2)', {'p0': 'おお', 'p0': 'どわーｗふ', 'p0': 'ぬ'}
        """
        query = Query()

        query += f"{column.name} IN ("
        if isinstance(value_list, SubQuery):
            query += value_list
        else:
            value_query_list = [Query() * column.constraint.get_sql_value(value_) for value_ in value_list]
            joined_value_query = query_join_comma(value_query_list)
            query += joined_value_query
        query += ")"

        self.straight_set(query)


class Range(CondCond):
    def __init__(
        self,
        column: Column,
        start_value: str | int | bytes | Path | datetime.date | Column | SubQuery,
        end_value: str | int | bytes | Path | datetime.date | Column | SubQuery,
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


class TRUE(CondCond):
    def __init__(self):
        """単位元: true"""
        query = Query("1")

        self.straight_set(query)


class FALSE(CondCond):
    def __init__(self):
        """単位元: false"""
        query = Query("0")

        self.straight_set(query)


# サブクエリ用
class Exists(CondCond):
    def __init__(self, value: SubQuery):
        """EXISTS"""
        query = Query()
        query += "EXISTS ("
        query += value
        query += ")"
        self.straight_set(query)
