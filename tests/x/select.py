# パターン1 fromありでカラム指定なし
# select = post.select()
# select.fetchone(dict_output=True)

# パターン2 fromありでカラム指定あり
# select = post.select(post.content_column)
# select.fetchall(dict_output=True)

# パターン3 whereにcond
# where = sql_module.conds.Contains(post.content_column, "天使ほま")
# select = post.select(post.content_column, where)
# select.fetchall(dict_output=True)

# パターン4 式にcond
# select = post.select(sql_module.funcs.Count(post.content_column))
# select.fetchone_value()

# パターン5 where+exists
# select_pre = post.select(1, sql_module.conds.Range(post.date_column, datetime.date(2026, 2, 11), datetime.date.today()))
# exists = sql_module.conds.Exists(select_pre)
# select = post.select(exists, is_from=False)
# select.fetchone_value()

# パターン6 join
# target_screen_name = "otomachiuna"
# join = [sql_module.Join(user_screen_name.screen_name_id_column), sql_module.Join(post.user_id_column, target_column=user_screen_name.user_id_column)]
# where = sql_module.conds.Eq(screen_name.screen_name_column, target_screen_name)
# select = post.select(post.id_column, join=join, where=where)
# select.fetchall(dict_output=True)


# パターン7 group_by
# select = post.select(sql_module.funcs.Count(post.favorite_count_column), group_by=post.favorite_count_column)
# select.fetchall(dict_output=True)

# パターン8 ページングシーク
# last_date = datetime.date(2100, 1, 1)
# last_id = 10**10
# limit = 8
# def get_seek_page(post, last_date: str, last_id: int, limit: int):

#     where = sql_module.conds.Less(post.date_column, last_date) | sql_module.conds.Eq(post.date_column, last_date) & sql_module.conds.Less(post.id_column, last_id)
#     order_by = [sql_module.OrderBy(post.date_column, False), sql_module.OrderBy(post.id_column, False)]

#     select = post.select(None, where, order_by=order_by, limit=limit)
#     print(select)
#     fetchall = select.fetchall(dict_output=True)
#     return fetchall, fetchall[-1]["date"], fetchall[-1]["id"] # out of rangeするまでできると思う
# fetchall1, last_date, last_id = get_seek_page(post, last_date, last_id, limit)
# fetchall2, last_date, last_id = get_seek_page(post, last_date, last_id, limit)
# fetchall3, last_date, last_id = get_seek_page(post, last_date, last_id, limit)

# パターン9 limit
# select = post.select(limit=8)
# select.fetchall(dict_output=True)
