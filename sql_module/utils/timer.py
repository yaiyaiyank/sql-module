from dataclasses import dataclass
from typing import Literal
import time

from sql_module import utils


@dataclass
class Timer:
    start: float = None
    end: float = None
    time_log: utils.LogLike | None = None

    def __post_init__(self):
        self.start = time.time()

    def finish(self, time_title: str, decimal_places: int = 6, limit_warning_sec: int = 5):
        self.end = time.time()
        if not self.time_log is None:
            # 結果をログる
            result_time = self.end - self.start
            self.time_log.debug(f"{time_title}: {result_time:.{decimal_places}f}[s]")
            # 設定した時間よりながければwarning
            if limit_warning_sec < result_time:
                self.time_log.warning(f"{time_title}: 実行時間が長いです。")

    def no(self, time_title: str):
        if not self.time_log is None:
            self.time_log.debug(f"{time_title}: 実行をスキップしました。")
