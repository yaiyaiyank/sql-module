class LogLike:
    """
    ログクラスのダミー。print・型ヒント用
    他ライブラリのオブジェクトとして、logging_module.Logオブジェクトなどを注入できる
    """

    def debug(self, string: str):
        print(string)

    def info(self, string: str):
        print(string)

    def warning(self, string: str):
        print(string)

    def error(self, string: str):
        print(string)

    def critical(self, string: str):
        print(string)
