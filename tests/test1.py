import sys
from pathlib import Path

sys.path.append(Path.cwd().__str__())

import sql_module


class Work(sql_module.AtIDTableDefinition):
    def set_colmun_difinition(self):
        self.name_column = self.table.get_column("name", str)
        self.user_id_column = self.table.get_column("user_id", int)


database = sql_module.SQLiteDataBase()
work = database.get_table_definition("work", Work)
work.create()
