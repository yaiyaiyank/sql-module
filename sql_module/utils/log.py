class PrintLog:
    """debugやinfoをもつログクラスのダミー。型ヒント用"""

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
