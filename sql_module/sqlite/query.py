from dataclasses import dataclass, field
from sql_module.sqlite.driver import Driver


@dataclass
class Querable:
    driver: Driver
    query: str
    placeholder_dict: dict = field(default_factory=dict)

    def execute(self):
        self.driver.execute(self.query, self.placeholder_dict)
