from yai.entry import Log, dataclass, field, Path, datetime, Literal, base_utils_module
from .driver import Driver


@dataclass
class Querable:
    driver: Driver
    query: str
    placeholder_dict: dict = field(default_factory=dict)

    def execute(self):
        self.driver.execute_cursor(self.query, self.placeholder_dict)
