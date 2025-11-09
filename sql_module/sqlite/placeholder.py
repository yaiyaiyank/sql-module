from yai.entry import Log, dataclass, field, Path, datetime, base_utils_module


@dataclass
class PlaceHolderable:
    placeholder_dict: dict = field(default_factory=dict)
    log: Log = field(default_factory=Log)

    def _add_placeholder(self, sql_value: list) -> str:
        """
        例
        self.placeholder_dict: {'p0': 5}
        sql_value: 'うおｗ'
        ->
        self.placeholder_dictを{'p0': 5, 'p1': 'うおｗ'}にして
        ':p1' を返す

        変数名がpというふうに可読性が終わっているのはクエリの文字数の上限により到達しにくくするためである
        """
        place_holder_key = f":p{self.placeholder_dict.__len__()}"
        self.placeholder_dict[place_holder_key] = sql_value
        return place_holder_key
