import sys
from pathlib import Path
import datetime

sys.path.append(Path.cwd().__str__())
import sql_module

database = sql_module.SQLiteDataBase()


class User(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", str, default_value="2")


class Channel(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", str, default_value="3")


user: User = database.get_table_definition("user", User)
channel: Channel = database.get_table_definition("channel", Channel)


class Work(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", str, default_value="4", unique=True)
        self.date_column = self.table.get_column("date", datetime.datetime, default_value=datetime.datetime(1070, 1, 1))
        self.user_id_column = self.table.get_column("user_id", int, references=user.id_column)
        self.channel_id_column = self.table.get_column("channel_id", int, references=channel.id_column)
        self.b = self.table.get_column("b", bytes, unique=True)


work: Work = database.get_table_definition("work", Work)

unique2 = sql_module.UniqueCompositeConstraint(work.user_id_column, work.date_column)
work.create(unique2)
self = work.table.info()

for raw_column in self.raw_column_list:
    print(raw_column)

for raw_index in self.raw_index_list:
    print(raw_index)

for raw_foreign_key in self.raw_foreign_key_list:
    print(raw_foreign_key)

work.table.driver.execute("PRAGMA index_info(sqlite_autoindex_work_3);")
index_info = work.table.driver.fetchall(True)
index_info
