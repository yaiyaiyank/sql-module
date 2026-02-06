"""info用にめちゃくちゃ適当な値を入れている"""

import sql_module
import datetime
from dataclasses import dataclass, field

database = sql_module.SQLiteDataBase()


class User(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", str, default_value="2")


user: User = database.get_table_definition(User)


class Channel(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", int, default_value=2)


channel: Channel = database.get_table_definition(Channel)


class Work(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", str, default_value="4", unique=True)
        self.date_column = self.table.get_column("date", datetime.datetime, default_value=datetime.datetime(1070, 1, 1))
        self.user_id_column = self.table.get_column("user_id", int, references=user.id_column, not_null=True)
        self.channel_id_column = self.table.get_column("channel_id", int, references=channel.id_column)
        self.b_column = self.table.get_column("b", bytes, unique=True)


work: Work = database.get_table_definition(Work)

unique2 = sql_module.UniqueCompositeConstraint(work.user_id_column, work.date_column)

user.create()
channel.create()
work.create(unique2)
work.name_column.make_index()
work.table.make_index([work.date_column, work.b_column])
info = work.table.info()
