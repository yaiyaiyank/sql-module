from dataclasses import dataclass
from abc import abstractmethod

from sql_module.exceptions import ConstraintConflictError
from sql_module import Column, utils


class CompositeConstraint:
    """複合キー制約"""

    def __init__(self, *args: Column):
        self.column_list: list[Column] = list(args)
        self.valid()

    @abstractmethod
    def valid(self):
        pass

    @abstractmethod
    def get_query(self) -> str:
        pass

    def get_column_name_list(self) -> list[str]:
        """
        [Column('site_id'), Column('content_id')]
        ->
        ['site_id', 'content_id']
        """
        column_name_list = [column.name.now for column in self.column_list]
        return column_name_list


class UniqueCompositeConstraint(CompositeConstraint):
    """
    複合ユニーク

    Tips:
        1つ目のカラムは左端プレフィックスにより有利になりやすい
    """

    def valid(self):
        # 個別にunique設定していた場合はraise
        for column in self.column_list:
            if column.constraint.unique:
                raise ConstraintConflictError("uniqueキーの列制約と表制約を同時に入れることはできません。")

    def get_query(self) -> str:
        """
        column_names = ['site_id', 'content_id']
        ->
        'UNIQUE (site_id, content_id)'
        """
        column_name_list = self.get_column_name_list()
        column_names = utils.join_comma(column_name_list)
        command = f"UNIQUE ({column_names})"
        return command


class PrimaryCompositeConstraint(CompositeConstraint):
    """複合主キー(個人的に主キーはサロゲートキーでないと変更に弱いので非推奨)"""

    def valid(self):
        # 個別にprimary設定していた場合はNone
        for column in self.column_list:
            if column.constraint.primary:
                raise ConstraintConflictError("primaryキーの列制約と表制約を同時に入れることはできません。")

    def get_query(self) -> str:
        """
        column_names = ['site_id', 'content_id']
        ->
        'PRIMARY KEY (site_id, content_id)'
        """
        column_name_list = self.get_column_name_list()
        column_names = utils.join_comma(column_name_list)
        command = f"PRIMARY KEY ({column_names})"
        return command
