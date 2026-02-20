from sql_module import utils, Driver, Query, conds
from sql_module.sqlite.table.column.name import ColumnName


class IndexQueryBuilder:
    def __init__(self, driver: Driver):
        """
        クエリ例:
        CREATE UNIQUE INDEX ux_customer_current
        ON customer_scd2(customer_id)
        WHERE is_current = 1
        """
        self.driver = driver

    def get_create_head_query(self, exists_ok: bool, is_unique: bool) -> Query:
        """最初のクエリ作成"""
        if is_unique:
            unique_string = " UNIQUE"
        else:
            unique_string = ""
        if exists_ok:
            exists_ok_string = " IF NOT EXISTS"
        else:
            exists_ok_string = ""

        return Query(f"CREATE{unique_string} INDEX{exists_ok_string}", driver=self.driver)

    def get_delete_head_query(self, not_exists_ok: bool) -> Query:
        """最初のクエリ作成"""
        if not_exists_ok:
            exists_ok_string = " IF EXISTS"
        else:
            exists_ok_string = ""

        return Query(f"DROP INDEX{exists_ok_string}", driver=self.driver)

    def get_index_name(
        self, column_name_list: list[ColumnName], index_name: str | None = None, is_unique: bool = False
    ):
        if not index_name is None:
            return index_name
        table_name = column_name_list[0].table_name
        columns_name = utils.join_under(sorted([column_name.name for column_name in column_name_list]))

        if is_unique:
            head_name = "ux"
        else:
            head_name = "idx"

        return f"{head_name}_{table_name}_{columns_name}"

    def get_on_query(self, column_name_list: list[ColumnName]):
        table_name = column_name_list[0].table_name
        columns_query = utils.join_comma([column_name.name for column_name in column_name_list])

        return Query(f"ON {table_name}({columns_query})")

    def get_where_query(self, where: conds.Cond | None) -> Query:
        if where is None:
            return Query()

        return Query("WHERE " + where.substitute())
