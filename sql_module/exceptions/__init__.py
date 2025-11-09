class SQLException(Exception):
    """SQL系の基底例外"""


class FetchNotFoundError(SQLException):
    """Fetchするものがなかった"""


class ColumnAlreadyRegistrationError(SQLException):
    """既にカラム登録しているときのエラー"""


class ConstraintConflictError(SQLException):
    """キー制約の組み合わせが不正なときのエラー"""


class SQLTypeError(SQLException):
    "そのPythonの型に対応するsqlite用の型がないときのエラー"
