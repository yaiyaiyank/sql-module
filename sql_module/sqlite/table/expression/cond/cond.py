# 標準ライブラリ
from pathlib import Path
from dataclasses import dataclass
import datetime
from typing import Self

from sql_module import Query, query_join_comma, expressions, funcs
from sql_module.sqlite.table.sub_query import SubQuery
from sql_module.sqlite.table.column.interface import ColumnLike


class Cond(Query, expressions.Expression):
    """条件"""

    def __and__(self, other: Self) -> Self:
        return And(self, other)

    def __or__(self, other: Self) -> Self:
        return Or(self, other)

    def __invert__(self):
        return Not(self)


class CondBool(Cond):
    pass


class CondCond(Cond):
    def get_value_query(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ) -> Query:
        query = Query()
        # カラムなら列比較
        if isinstance(value, ColumnLike):
            query += value.name.to_query(is_column_only_name)
            return query
        if isinstance(value, SubQuery):
            query += "("
            query += value
            query += ")"
            return query
        # 値
        if isinstance(column, ColumnLike):
            query *= column.constraint.get_sql_value(value)
            return query
        # havingなどで使う
        if isinstance(column, funcs.Func):
            if not isinstance(value, int):
                raise ValueError("having句ではintのみ対応です。")
            query *= value
            return query
            # インデックス生成のwhereでint以外の値使わんだろうしこれでいい

        raise ValueError(f"{column} ・ {value}の組み合わせのCondオブジェクトは作れません。")

    def get_column_name_query(self, column: ColumnLike | funcs.Func, is_column_only_name: bool = False) -> Query:
        if isinstance(column, ColumnLike):
            return column.name.to_query(is_column_only_name)
        if isinstance(column, funcs.Func):
            return column

        raise ValueError(f"{column} はカラム名クエリに変換できません。")


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
        # Trueの場合はそのままでいい
        if isinstance(left_where, TRUE):
            self.straight_set(right_where)
            return
        if isinstance(right_where, TRUE):
            self.straight_set(left_where)
            return

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
        # Falseの場合はそのままでいい
        if isinstance(left_where, FALSE):
            self.straight_set(right_where)
            return
        if isinstance(right_where, FALSE):
            self.straight_set(left_where)
            return

        query = left_where + " OR " + right_where

        self.straight_set(query)


class Not(CondBool):
    def __init__(self, where: Cond):
        if not isinstance(where, Cond):
            raise TypeError(f"演算: '~'はCondオブジェクトでのみ行えます。'~{where.__class__.__name__}'はできません。")

        query = "NOT (" + where + ")"
        self.straight_set(query)


class Eq(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery | None,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'user.id = :p0', {'p0': 2}
        'user.created_at = :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        query += self.get_column_name_query(column, is_column_only_name)
        if value is None:
            query += " IS NULL"
            self.straight_set(query)
            return

        query += " = "
        query += self.get_value_query(column, value, is_column_only_name)

        self.straight_set(query)


class GreaterEq(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'user.age >= :p0', {'p0': 20}
        'user.created_at >= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += " >= "
        query += self.get_value_query(column, value, is_column_only_name)

        self.straight_set(query)


class Greater(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'user.age > :p0', {'p0': 20}
        'user.created_at > :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += " > "
        query += self.get_value_query(column, value, is_column_only_name)

        self.straight_set(query)


class LessEq(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'user.age <= :p0', (20,)
        'user.created_at <= :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += " <= "
        query += self.get_value_query(column, value, is_column_only_name)

        self.straight_set(query)


class Less(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'user.age < :p0', (20,)
        'user.created_at < :p0', {'p0': '2025-03-04 22:44:59'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += " < "
        query += self.get_value_query(column, value, is_column_only_name)

        self.straight_set(query)


class StartsWith(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'device.bender LIKE :p0%', {'p0': 'Xiaomi'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += " LIKE "
        query += self.get_value_query(column, value, is_column_only_name)
        query += " || '%'"

        self.straight_set(query)


class EndsWith(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'device.bender LIKE %:p0', {'p0': 'ホールディングス'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += "LIKE '%' || "
        query += self.get_value_query(column, value, is_column_only_name)

        self.straight_set(query)


class Contains(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'device.bender LIKE %:p0', {'p0': 'うおｗ'}
        """
        query = Query()
        if value is None:
            raise TypeError("None禁止")

        query += self.get_column_name_query(column, is_column_only_name)
        query += "LIKE '%' || "
        query += self.get_value_query(column, value, is_column_only_name)
        query += " || '%'"

        self.straight_set(query)


class In(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        value_list: list[str | int | bytes | Path | datetime.date | None] | SubQuery,
        is_column_only_name: bool = False,
    ):
        """
        例:
        'channel.name IN (:p0, :p1, :p2)', {'p0': 'おお', 'p0': 'どわーｗふ', 'p0': 'ぬ'}
        """
        query = Query()

        query += self.get_column_name_query(column, is_column_only_name)
        query += " IN ("
        if isinstance(value_list, SubQuery):
            query += value_list
        else:
            value_query_list = [self.get_value_query(column, value, is_column_only_name) for value in value_list]
            joined_value_query = query_join_comma(value_query_list)
            query += joined_value_query
        query += ")"

        self.straight_set(query)


class Range(CondCond):
    def __init__(
        self,
        column: ColumnLike | funcs.Func,
        start_value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery | None,
        end_value: str | int | bytes | Path | datetime.date | ColumnLike | SubQuery | None,
        include_start: bool = True,
        include_end: bool = False,
        is_column_only_name: bool = True,
    ):
        """
        例:
        'user.updated_at >= :p0 AND user.updated_at < :p1', {'p0': '2022-02-22 00:00:00', 'p1': '2022-02-23 00:00:00'}
        """
        if start_value is None:
            greater = TRUE()
        elif include_start:
            greater = GreaterEq(column, start_value, is_column_only_name)
        else:
            greater = Greater(column, start_value, is_column_only_name)
        if end_value is None:
            less = TRUE()
        elif include_end:
            less = LessEq(column, end_value), is_column_only_name
        else:
            less = Less(column, end_value, is_column_only_name)

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
