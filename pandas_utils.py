import pandas as pd
import utils
from global_vars import *


def exec_join(df1, df2, attr_lst1, attr_lst2):
    return pd.DataFrame.merge(df1, df2, left_on=attr_lst1, right_on=attr_lst2, how="inner")


def df_columns_rename(data_frame,table,separator):
    old_columns = list(data_frame.columns.values)
    new_columns = [table + separator + x for x in old_columns]
    data_frame.columns = new_columns


def do_join(to_join_lst):
    df_map={}
    join_lst = []
    for i in range(0, len(to_join_lst)):
        join_lst.append(to_join_lst[i])
    from_dict = dict()
    for i in range(0, len(join_lst)):
        tb1 = utils.extract_ta(join_lst[i][0])[0]
        tb2 = utils.extract_ta(join_lst[i][2])[0]
        if tb1 < tb2:
            if (tb1, tb2) not in from_dict:
                from_dict[(tb1, tb2)] = [[join_lst[i][0], join_lst[i][2]]]
            else:
                from_dict[(tb1, tb2)].append([join_lst[i][0], join_lst[i][2]])
        else:
            if (tb1, tb2) not in from_dict:
                from_dict[(tb2, tb1)] = [[join_lst[i][2], join_lst[i][0]]]
            else:
                from_dict[(tb2, tb1)].append([join_lst[i][2], join_lst[i][0]])
    used_tbls = set()
    all_df = pd.DataFrame
    for x in from_dict.keys():
        attr_lst1 = [y[0] for y in from_dict[x]]
        attr_lst2 = [y[1] for y in from_dict[x]]
        if x[0] in alias_cast_dict.keys():
            attr_lst1 = [extract_ta(y)[1] for y in attr_lst1]
        if x[1] in alias_cast_dict.keys():
            attr_lst2 = [extract_ta(y)[1] for y in attr_lst2]
        if x[0] in used_tbls:
            used_tbls.add(x[1])
            all_df = exec_join(all_df, df_map[x[1]], attr_lst1, attr_lst2)
        elif x[1] in used_tbls:
            used_tbls.add(x[0])
            all_df = exec_join(df_map[x[0]], all_df, attr_lst1, attr_lst2)
        else:
            used_tbls.add(x[0])
            used_tbls.add(x[1])
            all_df = exec_join(df_map[x[0]], df_map[x[1]], attr_lst1, attr_lst2)
