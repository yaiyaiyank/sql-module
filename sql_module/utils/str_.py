import re


def remove_invalid_path_chars(string: str) -> str:
    """ファイル名として使用できない文字を削除する"""
    # Windowsでファイル名として使用できない文字を定義
    invalid_chars = r'[<>:"/\\|?*]'
    # 無効な文字を除去
    return re.sub(invalid_chars, "", string)


def join_comma(str_list: list[str], no_empty: bool = False) -> str:
    """
    ['id INTEGER PRIMARY KEY AUTOINCREMENT', 'name TEXT']
    ->
    'id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT'
    """
    if no_empty:
        str_list = [str_ for str_ in str_list if str_ != ""]

    joined_str = ", ".join(str_list)
    return joined_str


def join_under(str_list: list[str], no_empty: bool = False) -> str:
    """
    ['read', 'text']
    ->
    'read_text'
    """
    if no_empty:
        str_list = [str_ for str_ in str_list if str_ != ""]

    joined_str = "_".join(str_list)
    return joined_str


def join_space(str_list: list[str], no_empty: bool = False) -> str:
    """
    ['read', 'text']
    ->
    'read text'
    """
    if no_empty:
        str_list = [str_ for str_ in str_list if str_ != ""]

    joined_str = " ".join(str_list)
    return joined_str


def camel_to_snake(s: str) -> str:
    """
    'RandomWork'
    ->
    'random_work'

    チャッピー製。
    """
    # 例: "HTTPRequest" -> "HTTP_Request" の段階を作ってから
    s = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", s)
    # 例: "HTTP_Request" -> "HTTP__Request" みたいにならないように整えて
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()
