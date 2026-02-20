from pathlib import Path
from dataclasses import dataclass, field
import sqlite3
import time
from typing import Self, Literal

from sql_module import utils, exceptions


@dataclass
class Status:
    conn: bool = False
    cursor: bool = False

    def info_status_text(self):
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
    database_file_path: Path | str | None
    status: Status = field(default_factory=Status)

    def __post_init__(self):
        if self.database_file_path is None:
            self.database_file_path = ":memory:"

    def __repr__(self) -> str:
        status_text = self.status.info_status_text()
        db_path_text = f"db_path: {self.database_file_path}"
        return f"{status_text} {db_path_text}"

    def open_full(self):
        """
        conn -> cursor の順にopen
        """
        if not self.status.conn:
            self.open_conn()
            # これをしないと外部キー制約がオフになったまま(connect時毎回必要)
            self.execute("PRAGMA foreign_keys = ON")
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

    def execute(
        self,
        query: str,
        parameters: dict[str] | None = None,
        time_log: utils.LogLike | None = None,
    ):
        """
        cursorを実行

        Args:
            query (str): クエリ
            parameters (dict[str] | None): プレースホルダのパラメータ
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        timer = utils.Timer(time_log=time_log)

        self.open_full()
        if parameters is None or parameters.__len__() == 0:
            self.cursor.execute(query)
        else:
            self.cursor.execute(query, parameters)

        timer.finish("実行時間")

    def executemany(
        self,
        query: str,
        parameters: list[dict[str]],
        time_log: utils.LogLike | None = None,
    ):
        """
        cursorを実行

        Args:
            query (str): クエリ
            parameters (dict[str] | None): プレースホルダのパラメータ
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        timer = utils.Timer(time_log=time_log)

        # バルクには必要
        if parameters is None:
            raise exceptions.BulkError("バルクはパラメータが必要です。")

        # パラメータの長さ0だとバルクする意味がない
        if parameters.__len__() == 0:
            timer.no("バルク実行・パラメータ数が0")
            return

        self.open_full()
        self.cursor.executemany(query, parameters)

        timer.finish("バルク実行")

    def rollback(self):
        self.conn.rollback()

    def commit(self, time_log: utils.LogLike | None = None):
        """
        コミットする。
        Args:
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        timer = utils.Timer(time_log=time_log)

        try:
            self.conn.commit()
        except sqlite3.OperationalError:
            self.rollback()
            # 再起動する
            self.close_full()
            self.open_full()
            raise

        timer.finish("コミット時間")

    def fetchall(
        self, dict_output: bool = False, time_log: utils.LogLike | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        """
        全行取り出す
        Args:
            dict_output (bool): 辞書で出力
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        timer = utils.Timer(time_log=time_log)

        fetchall_list = self.cursor.fetchall()
        if dict_output:
            fetchall_list = [dict(fetch) for fetch in fetchall_list]

        timer.finish("fetchall時間")

        return fetchall_list

    def fetchmany(
        self, limit: int, dict_output: bool = False, time_log: utils.LogLike | None = None
    ) -> list[dict[str]] | list[sqlite3.Row]:
        """
        何行か取り出す
        Args:
            dict_output (bool): 辞書で出力
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        timer = utils.Timer(time_log=time_log)

        fetchmany_list = self.cursor.fetchmany(limit)
        if dict_output:
            fetchmany_list = [dict(fetch) for fetch in fetchmany_list]

        timer.finish("fetchmany時間")

        return fetchmany_list

    def fetchone(self, dict_output: bool = False, time_log: utils.LogLike | None = None) -> dict[str] | sqlite3.Row:
        """
        1行取り出す
        Args:
            dict_output (bool): 辞書で出力
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        timer = utils.Timer(time_log=time_log)

        fetchone = self.cursor.fetchone()
        if fetchone is None:
            raise exceptions.FetchNotFoundError
        if dict_output:
            fetchone = dict(fetchone)

        timer.finish("fetchone時間")

        return fetchone

    def fetchgrid(
        self, limit: int, dict_output: bool = False, time_log: utils.LogLike | None = None
    ) -> list[list[dict[str]]] | list[list[sqlite3.Row]]:
        """
        全行をlimitごとに取り出してリストに分ける
        Args:
            dict_output (bool): 辞書で出力
            time_log (ログ系オブジェクト | None): 時間計測のログ。
            - Noneなら出さない
            - 'print_log'ならprintだけする
            - ログ系オブジェクト(debugメソッドなどを持つ)なら、そのログを使う。
        """
        all_list = []
        timer = utils.Timer(time_log=time_log)

        while True:
            fetchmany_list = self.cursor.fetchmany(limit)
            # fetchmany_listがなくなるまで
            if fetchmany_list.__len__() == 0:
                break

            if dict_output:
                fetchmany_list = [dict(fetch) for fetch in fetchmany_list]
            all_list.append(fetchmany_list)

        timer.finish("fetchmany時間")

        return all_list
