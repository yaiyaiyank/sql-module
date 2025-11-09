from pathlib import Path
from dataclasses import dataclass, field
import sqlite3
import time
from typing import Self

from ..exceptions import FetchNotFoundError


@dataclass
class Status:
    conn: bool = False
    cursor: bool = False

    def make_repr_text(self):
        if self.conn:
            conn_oc = "conn is open"
        else:
            conn_oc = "conn is close"
        if self.cursor:
            cursor_oc = "cursor is open"
        else:
            cursor_oc = "cursor is close"

        text = f"状態: {conn_oc}, {cursor_oc}"
        return text


@dataclass
class Driver:
    database_file_path: Path | str
    status: Status = field(default_factory=Status)

    def __repr__(self) -> str:
        text = self.status.make_repr_text()
        return text

    def open_full(self):
        """
        conn -> cursor の順にopen
        """
        if not self.status.conn:
            self.open_conn()
            # これをしないと外部キー制約がオフになったまま(connect時毎回必要)
            self.execute_cursor("PRAGMA foreign_keys = ON")
            # これをしないとデフォルトではfetchがタプルで帰ってきてしまう
        if not self.status.cursor:
            self.open_cursor()

    def close_full(self):
        """
        cursor -> conn の順にclose
        """
        if self.status.cursor:
            self.close_cursor()
        if self.status.conn:
            self.close_conn()

    def open_conn(self):
        self.conn = sqlite3.connect(self.database_file_path)
        self.status.conn = True

    def close_conn(self):
        self.conn.close()
        self.status.conn = False

    def open_cursor(self):
        self.cursor = self.conn.cursor()
        self.cursor.row_factory = sqlite3.Row  # sqlite3.Rowオブジェクトはdictと同等以上の機能があるが、row: sqlite3.Rowオブジェクトとしてisinstance(row, dict)ではFalseだった。isinstance(row, list)でもFalseだった。
        self.status.cursor = True

    def close_cursor(self):
        self.cursor.close()
        self.status.cursor = False

    def begin(self):
        self.conn.execute("BEGIN")

    def execute_cursor(self, query: str, parameters: dict[str] | None = None, time_log: bool = False):
        """
        cursorを実行

        Args:
            query (str): クエリ
            parameters (dict[str] | None): プレースホルダのパラメータ
        """
        self.open_full()
        time_start = time.time()
        if parameters is None or parameters.__len__() == 0:
            self.cursor.execute(query)
        # Noneや__len__() > 0
        else:
            self.cursor.execute(query, parameters)
        time_end = time.time()
        if time_log:
            print(f"実行時間: {time_end - time_start:10f}s")

    def rollback(self):
        self.conn.rollback()

    def commit(self, time_log: bool = False):
        time_start = time.time()
        try:
            self.conn.commit()
        except sqlite3.OperationalError:
            # 再起動する
            self.close_full()
            self.open_full()
            raise
        time_end = time.time()
        if time_log:
            print(f"コミット時間: {time_end - time_start:10f}s")

    def fetchall(self, dict_output: bool = False, time_log: bool = False) -> list[dict[str]]:
        """全行取り出す"""
        time_start = time.time()
        fetchall_list = self.cursor.fetchall()
        if dict_output:
            fetchall_list = [dict(fetch) for fetch in fetchall_list]
        time_end = time.time()
        if time_log:
            print(f"fetch時間: {time_end - time_start:10f}s")
        return fetchall_list

    def fetchone(self, dict_output: bool = False, time_log: bool = False) -> dict[str]:
        """1行取り出す"""
        time_start = time.time()
        fetchone = self.cursor.fetchone()
        if fetchone is None:
            raise FetchNotFoundError
        if dict_output:
            fetchone = dict(fetchone)
        time_end = time.time()
        if time_log:
            print(f"fetch時間: {time_end - time_start:10f}s")
        return fetchone

    def fetchmany(self, rows_count: int, dict_output: bool = False, time_log: bool = False) -> list[dict[str]]:
        """何行か取り出す"""
        time_start = time.time()
        fetchmany_list = self.cursor.fetchmany(rows_count)
        if dict_output:
            fetchmany_list = [dict(fetch) for fetch in fetchmany_list]
        time_end = time.time()
        if time_log:
            print(f"fetch時間: {time_end - time_start:10f}s")
        return fetchmany_list
