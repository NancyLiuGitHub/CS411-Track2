from os.path import isdir, isfile, join
import os
import io
import dateutil.parser
import pandas as pd
import time

DEBUG = True

db_dir = "simpleDb"
db_dfs = dict()
tbl_map = dict()
df_map = dict()
# df_key_indices = dict()
# df_lens = dict()
joined_df = pd.DataFrame
whole_set = set()
alias_dict = dict()
alias_cast_dict = dict()
TMP_COL = "tmp##__"


def read_files(my_dir):
    if not isdir(my_dir):
        print("Cannot found given database!")
        return
    global db_dir
    db_dir = my_dir
    files = [join(my_dir, f) for f in os.listdir(my_dir) if isfile(join(my_dir, f))]
    for file in files:
        with io.open(file, "r", encoding='utf-8') as f:
            db_dfs[file] = pd.read_csv(f, encoding='utf-8')
            # df_lens[file] = get_set(len(db_dfs[file]) - 1)
    # for x in db_dfs.values():
    #     print(x)
    print("load ", my_dir, "successfully")
    print("Currently we have: ", files, " tables")


# def exec_where(my_df, attr_lst, value_lst, opr_lst, obj_lst, and_lst):
#     """
#     Get a filtered dataframe
#     :param and_lst: True, we want an and
#     :param obj_lst: True, if we want to compare with an attribute
#     :param my_df:
#     :param attr_lst: use which attribute to operate
#     :param value_lst: the value(attribute name) to compare
#     :param opr_lst: operation
#     :return: Filtered df
#     """
#     my_df = pd.DataFrame
#     # my_df.
#     # # cons = []
#     # for i in range(0,len(opr_lst)):
#     #
#     #
#     # return my_df[]


def get_df_by_set(my_df, my_set):
    return my_df.iloc[list(my_set), :]


def exec_dynamic_where(my_df, my_attr, other_attr, opr, extra_value=None, extra_opr=None):
    if extra_value is not None and extra_opr is not None:
        tmp_col = "tmp##__" + other_attr
        my_df[tmp_col] = float(extra_value)
        if extra_opr == '-':
            my_df[tmp_col] = my_df[other_attr] - my_df[tmp_col]
        elif extra_opr == '+':
            my_df[tmp_col] = my_df[other_attr] + my_df[tmp_col]
        elif extra_opr == '*':
            my_df[tmp_col] = my_df[other_attr] * my_df[tmp_col]
        elif extra_opr == '/':
            my_df[tmp_col] = my_df[other_attr] / my_df[tmp_col]

        if opr == '=':
            my_set = set(my_df[my_df[my_attr] == my_df[tmp_col]].index.values.tolist())
            # my_df.drop(tmp_col, axis=1, inplace=True)
            return my_set
        elif opr == '>':
            my_set = set(my_df[my_df[my_attr] > my_df[tmp_col]].index.values.tolist())
            # my_df.drop(tmp_col, axis=1, inplace=True)
            return my_set
        elif opr == '>=':
            my_set = set(my_df[my_df[my_attr] >= my_df[tmp_col]].index.values.tolist())
            # my_df.drop(tmp_col, axis=1, inplace=True)
            return my_set
        elif opr == '<':
            my_set = set(my_df[my_df[my_attr] < my_df[tmp_col]].index.values.tolist())
            # my_df.drop(tmp_col, axis=1, inplace=True)
            return my_set
        elif opr == '<=':
            my_set = set(my_df[my_df[my_attr] <= my_df[tmp_col]].index.values.tolist())
            # my_df.drop(tmp_col, axis=1, inplace=True)
            return my_set
        else:
            my_set = set(my_df[my_df[my_attr] != my_df[tmp_col]].index.values.tolist())
            # my_df.drop(tmp_col, axis=1, inplace=True)
            return my_set

    else:
        if opr == '=':
            return set(my_df[my_df[my_attr] == my_df[other_attr]].index.values.tolist())
        elif opr == '>':
            return set(my_df[my_df[my_attr] > my_df[other_attr]].index.values.tolist())
        elif opr == '>=':
            return set(my_df[my_df[my_attr] >= my_df[other_attr]].index.values.tolist())
        elif opr == '<':
            return set(my_df[my_df[my_attr] < my_df[other_attr]].index.values.tolist())
        elif opr == '<=':
            return set(my_df[my_df[my_attr] <= my_df[other_attr]].index.values.tolist())
        else:
            return set(my_df[my_df[my_attr] != my_df[other_attr]].index.values.tolist())

            # if opr == '=':
            #     return my_df[my_df[my_attr] == my_df[other_attr]]
            #     return set(my_df[my_df[my_attr] == my_df[other_attr]].index.values.tolist())
            # elif opr == '>':
            #     return my_df[my_df[my_attr] > my_df[other_attr]]
            # elif opr == '>=':
            #     return my_df[my_df[my_attr] >= my_df[other_attr]]
            # elif opr == '<':
            #     return my_df[my_df[my_attr] < my_df[other_attr]]
            # elif opr == '<=':
            #     return my_df[my_df[my_attr] <= my_df[other_attr]]
            # else:
            #     return my_df[my_df[my_attr] != my_df[other_attr]]


def exec_fix_where(my_df, attr, value, opr, return_set=True):
    if return_set:
        if opr.upper() == 'LIKE':
            return exec_match(my_df, attr, value)
        if opr == '=':
            return set(my_df[my_df[attr] == value].index.values.tolist())
        elif opr == '>':
            return set(my_df[my_df[attr] > value].index.values.tolist())
        elif opr == '>=':
            return set(my_df[my_df[attr] >= value].index.values.tolist())
        elif opr == '<':
            return set(my_df[my_df[attr] < value].index.values.tolist())
        elif opr == '<=':
            return set(my_df[my_df[attr] <= value].index.values.tolist())
        else:
            return set(my_df[my_df[attr] != value].index.values.tolist())
    else:
        if opr.upper() == 'LIKE':
            return exec_match(my_df, attr, value,False)
        if opr == '=':
            return my_df[my_df[attr] == value]
        elif opr == '>':
            return my_df[my_df[attr] > value]
        elif opr == '>=':
            return my_df[my_df[attr] >= value]
        elif opr == '<':
            return my_df[my_df[attr] < value]
        elif opr == '<=':
            return my_df[my_df[attr] <= value]
        else:
            return my_df[my_df[attr] != value]


def exec_where(my_df, attr, value, opr):
    tbl, att = extract_ta(attr)
    if tbl in alias_cast_dict:
        attr = att
    if isinstance(value, list):
        return exec_dynamic_where(my_df, attr, value[0], opr, value[2], value[1])
    elif is_float(value) or is_date(value) or is_quoted(value):
        par = get_data(value)
        return exec_fix_where(my_df, attr, par, opr)
    else:
        return exec_dynamic_where(my_df, attr, value, opr)


# def convert_opr(obj1, obj2, opr):
#     if opr == "==":
#         return obj1


def exec_match(my_df, attr, string, return_set=True):
    # my_str="r'.*?"+string+".*'"
    strs = string.split('%')
    if len(strs) == 1:
        if return_set:
            return set(my_df[my_df[attr] == strs].index.values.tolist())
        else:
            return my_df[my_df[attr] == strs]
    if string[0] != '%':
        string = '^' + string
    if string[len(string) - 1] != '%':
        string = string + '$'
    to_query = string.replace("%", "[\s\S]*")
    # if DEBUG:
    #     print(to_query)
    #     df_print_all(my_df)
    # tmp = my_df[my_df[attr].str.contains(to_query, na=False)]
    tmp = my_df[my_df[attr].str.contains(to_query, na=False)]
    if return_set:
        return set(tmp.index.values.tolist())
    else:
        return tmp


def exec_select(my_df, attr_lst):
    selected_attrs=[]
    if attr_lst[0] == "*":
        all_cols = list(my_df.columns.values)
        for x in all_cols:
            if x.startswith(TMP_COL):
                continue
            selected_attrs.append(x)
        return my_df[selected_attrs]
    for attr in attr_lst:
        tbl, att = extract_ta(attr)
        if tbl in alias_cast_dict:
            attr = att
        selected_attrs.append(attr)
    selected_df = my_df[list(selected_attrs)]
    return selected_df


def exec_from(df1, df2, attr_lst1, attr_lst2):
    # if alias == ['']:
    return pd.DataFrame.merge(df1, df2, left_on=attr_lst1, right_on=attr_lst2, how="inner")
    # my_df = pd.DataFrame.merge(df1, df2, left_on=attr_lst1, right_on=attr_lst2, suffixes=alias, how="inner")
    # return my_df


def exec_sql(select_lst, from_lst, to_join_lst, where_lst, max_level):
    if DEBUG:
        start_time = time.time()
    global joined_df
    global whole_set
    global tbl_map
    global df_map
    global alias_dict
    global alias_cast_dict
    joined_df=pd.DataFrame
    tbl_map = dict()
    df_map = dict()
    joined_df = pd.DataFrame
    whole_set = set()
    alias_dict = dict()
    alias_cast_dict = dict()

    print("in exec_sql")
    if len(from_lst) > 1:
        parse_from(from_lst)
        join_lst = []
        # if len(to_join_lst)==0:
        #     for i in range(0, len(where_lst), 2):
        #         x = where_lst[i]
        #         # tb1=extract_ta(x[0])
        #         # tb2=extract_ta(x[2])
        #         if len(x) == 3 and x[1] == '=':
        #             if "." in x[2] and "." in x[0]:
        #                 join_lst.append(x)
        #             elif "." in x[2] and x[0] not in df_map[extract_ta(x[2])[0]].columns:
        #                 join_lst.append(x)
        #             elif "." in x[0] and x[2] not in df_map[extract_ta(x[0])[0]].columns:
        #                 join_lst.append(x)
        #             elif extract_ta(x[0])[0] != extract_ta(x[2])[0]:
        #                 join_lst.append(x)
        #             else:
        #                 break

        # first, extract them
        # if len(where_lst) > 0:
        #     pre_query(where_lst)

        for i in range(0, len(to_join_lst), 2):
            join_lst.append(to_join_lst[i])
        from_dict = dict()
        for i in range(0, len(join_lst)):
            tb1 = extract_ta(join_lst[i][0])[0]
            tb2 = extract_ta(join_lst[i][2])[0]
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
                all_df = exec_from(all_df, df_map[x[1]], attr_lst1, attr_lst2)
            elif x[1] in used_tbls:
                used_tbls.add(x[0])
                all_df = exec_from(df_map[x[0]], all_df, attr_lst1, attr_lst2)
            else:
                used_tbls.add(x[0])
                used_tbls.add(x[1])
                all_df = exec_from(df_map[x[0]], df_map[x[1]], attr_lst1, attr_lst2)

        # if len(join_lst) * 2 - 1 == len(where_lst):  # which means no "really" where applied, program ends
        if len(where_lst) == 0:  # which means no "really" where applied, program ends
        # if modified_where_list is None:  # which means no "really" where applied, program ends
            res_df = exec_select(all_df, select_lst)
            print(res_df)
        else:
            # where_lst = where_lst[len(join_lst) * 2:]
            joined_df = all_df
            whole_set = get_set(len(joined_df) - 1)
            if DEBUG:
                print("before query", time.time() - start_time)
                # print(joined_df)
            my_set = naive_query(where_lst, max_level)
            res_df = get_df_by_set(joined_df, my_set)
            # df_print_all(exec_select(res_df, select_lst))
            print(exec_select(res_df, select_lst))
    else:
        parse_from(from_lst)
        joined_df = df_map[from_lst[0][0]]
        if len(where_lst) != 0:
            my_set = naive_query(where_lst, max_level)
            res_df = get_df_by_set(joined_df, my_set)
            # df_print_all(exec_select(res_df, select_lst))
            print(exec_select(res_df, select_lst))
        else:
            # df_print_all(exec_select(joined_df, select_lst))
            print(exec_select(joined_df, select_lst))


def pre_query(conditions):
    """
    This method is used to test if a query's outermost level can be finished directly
    :param conditions:
    :return:
    """
    not_index = []
    or_index = []
    and_index = []
    if len(conditions) == 1:
        pre_check(conditions, 0)
    else:
        for i in range(len(conditions)):
            op = conditions[i]
            if op == "_0_not":
                not_index.append(i)
            if op == "_0_and":
                and_index.append(i)
            if op == "_0_or":
                or_index.append(i)
        if len(or_index) == 0 and len(not_index) == 0:  # If our assumption is true
            for i in range(0, len(conditions), 2):
                pre_check(conditions, i)
            return conditions
        else:
            return conditions


def pre_check(conditions, pos):
    if isinstance(conditions[pos], list):
        if isinstance(conditions[pos][2], list):  # which means it cannot be a fix value
            return
        if is_float(conditions[pos][2]) or is_quoted(conditions[pos][2]):
            p = get_data(conditions[pos][2])
            df_name, attr = extract_ta(conditions[pos][0])
            # tbl, att = extract_ta(str(attr))
            if df_name not in alias_cast_dict:
                attr = conditions[pos][0]
            df_map[df_name] = exec_fix_where(df_map[df_name], attr, p, conditions[pos][1], False)


def naive_query(conditions, max_level):
    """
    I call it naive query, since there is no optimization, only follow the order
    :param max_level: to tell how many levels exist
    :param conditions: the conditions to query
    :return: expected table with all attributes
    """
    if len(conditions) == 1:
        return exec_where(joined_df, conditions[0][0], conditions[0][2], conditions[0][1])
    df_key_indices = dict()
    key_id = 1

    for level_index in range(max_level, -1, -1):
        not_index = []
        and_index = []
        or_index = []
        # get and, or operator positions in a specific level
        for i in range(len(conditions)):
            op = conditions[i]
            if op == "_" + str(level_index) + "_not":
                not_index.append(i)
        while not_index:
            not_index.reverse()  # make sure the operations will not affect other indexes
            for i in not_index:
                if isinstance(conditions[i + 1], list):
                    cur_set = exec_where(joined_df, conditions[i + 1][0], conditions[i + 1][2], conditions[i + 1][1])
                else:
                    cur_set = df_key_indices[conditions[i + 1]]
                cur_set = whole_set - cur_set
                df_key_indices[key_id] = cur_set
                if len(conditions) == i + 2:
                    conditions = conditions[0:i] + [key_id]
                else:
                    conditions = conditions[0:i] + [key_id] + conditions[i + 2:]
                key_id += 1
                not_index.remove(i)
        for i in range(len(conditions)):
            op = conditions[i]
            if op == "_" + str(level_index) + "_and":
                and_index.append(i)
        while and_index:
            and_index.reverse()
            for i in and_index:
                if DEBUG:
                    start_time = time.time()

                if isinstance(conditions[i - 1], list):
                    left_set = exec_where(joined_df, conditions[i - 1][0], conditions[i - 1][2], conditions[i - 1][1])
                else:
                    left_set = df_key_indices[conditions[i - 1]]
                if isinstance(conditions[i + 1], list):
                    right_set = exec_where(joined_df, conditions[i + 1][0], conditions[i + 1][2], conditions[i + 1][1])
                else:
                    right_set = df_key_indices[conditions[i + 1]]
                cur_set = set.intersection(left_set, right_set)
                df_key_indices[key_id] = cur_set
                if len(conditions) == i + 2:
                    conditions = conditions[0:i - 1] + [key_id]
                else:
                    conditions = conditions[0:i - 1] + [key_id] + conditions[i + 2:]
                key_id += 1
                if DEBUG:
                    print("one round: ", time.time() - start_time)

            and_index.clear()

        for i in range(len(conditions)):
            op = conditions[i]
            if op == "_" + str(level_index) + "_or":
                or_index.append(i)
        while or_index:
            or_index.reverse()
            for i in or_index:
                if isinstance(conditions[i - 1], list):
                    left_set = exec_where(joined_df, conditions[i - 1][0], conditions[i - 1][2], conditions[i - 1][1])
                else:
                    left_set = df_key_indices[conditions[i - 1]]
                if isinstance(conditions[i + 1], list):
                    right_set = exec_where(joined_df, conditions[i + 1][0], conditions[i + 1][2], conditions[i + 1][1])
                else:
                    right_set = df_key_indices[conditions[i + 1]]
                cur_set = set.union(left_set, right_set)
                df_key_indices[key_id] = cur_set
                if len(conditions) == i + 2:
                    conditions = conditions[0:i - 1] + [key_id]
                else:
                    conditions = conditions[0:i - 1] + [key_id] + conditions[i + 2:]
                key_id += 1
                or_index.remove(i)

    return df_key_indices[key_id - 1]


# def optimize_query(conditions, max_level):
#     key_id = 1
#     for level_index in range(max_level, -1, -1):
#         not_index = []
#         and_index = []
#         or_index = []
#
#         # get and, or operator positions in a specific level
#         for i in range(len(conditions)):
#             op = conditions[i]
#             if op == "_" + str(level_index) + "_and":
#                 and_index.append(i)
#             if op == "_" + str(level_index) + "_or":
#                 or_index.append(i)
#             if op == "_" + str(level_index) + "_not":
#                 not_index.append(i)
#         if len(and_index) == 0 and len(or_index) == 0 and len(not_index) == 0:  # which means no and, or, not
#             if isinstance(conditions[i - 1], list) and (
#                         is_float(conditions[i - 1][2]) or is_quoted(conditions[i - 1][2])):
#                 p = get_data(conditions[i - 1][2])
#                 df_name, attr = extract_ta(conditions[i - 1][0])
#                 my_df = exec_fix_where(df_map[df_name].copy(), attr, p, conditions[i - 1][1])
#                 # if DEBUG:
#                 #     print(df_map[df_name])
#                 #     print(my_df)
#                 return my_df
#
#         # First compute any predicate that can be done in one index-based query
#         for i in and_index + or_index:
#             if isinstance(conditions[i - 1], list) and (
#                         is_float(conditions[i - 1][2]) or is_quoted(conditions[i - 1][2])):
#                 p = get_data(conditions[i - 1][2])
#                 df_name, attr = extract_ta(conditions[i - 1][0])
#                 my_set = exec_fix_where(df_map[df_name].copy(), attr, p, conditions[i - 1][1])
#                 df_key_indices[(key_id, df_name)] = my_set
#                 conditions = conditions[0:i - 1] + [(key_id, df_name)] + conditions[i:]
#                 key_id += 1
#
#                 # TODO: compute on predicate in conditions[i-1] as its third block is a number or quoted string.
#                 # Can use quoted_string[1:-1] to get the content, without outer quote markers.
#             if isinstance(conditions[i + 1], list) and (
#                         is_float(conditions[i + 1][2]) or is_quoted(conditions[i + 1][2])):
#                 p = get_data(conditions[i + 1][2])
#                 df_name, attr = extract_ta(conditions[i + 1][0])
#                 my_set = exec_fix_where(df_map[df_name].copy(), attr, p, conditions[i + 1][1])
#                 df_key_indices[(id, df_name)] = my_set
#                 if i + 1 >= len(conditions) - 1:
#                     conditions = conditions[0:i + 1] + [(key_id, df_name)]
#                 else:
#                     conditions = conditions[0:i + 1] + [(key_id, df_name)] + conditions[i + 2:]
#                 key_id += 1
#             print("Wait")
#         for i in not_index:
#             if is_float(conditions[i + 1][2]) or is_quoted(conditions[i + 1][2]):
#                 print("not")
#                 p = get_data(conditions[i + 1][2])
#                 df_name, attr = extract_ta(conditions[i + 1][0])
#                 my_set = exec_fix_where(df_map[df_name].copy(), attr, p, conditions[i + 1][1])
#                 df_key_indices[(key_id, df_name)] = df_lens[tbl_map[df_name]] - my_set
#                 if i + 1 >= len(conditions) - 1:
#                     conditions = conditions[0:i] + [(key_id, df_name)]  # fixme: not sure your original thoughts
#                 else:
#                     conditions = conditions[0:i] + [(key_id, df_name)] + conditions[
#                                                                          i + 2:]  # fixme: not sure your original thoughts
#                 key_id += 1
#                 not_index.remove(i)  # fixme: not sure your original thoughts
#         # Order: Not -> And -> Or
#         while not_index:
#             for i in and_index:
#                 # TODO: compute NOT with the next block in conditions list, which could be a predicate or a key.
#                 not_index.remove(i)
#
#         while and_index:
#             for i in and_index:
#                 if not isinstance(conditions[i - 1], list) and not isinstance(conditions[i + 1], list):
#                     # compute AND when both sides are computed sets and set three block in conditions to one block of key.
#                     new_set = set.intersection(df_key_indices[conditions[i - 1]], df_key_indices[conditions[i + 1]])
#                     and_index.remove(i)
#                     conditions = conditions[0:i] + [(key_id, df_name)]
#                     break
#                 elif not isinstance(conditions[i - 1], list) and conditions[i + 1][1] == '=':
#                     # TODO: (Join)if left block has filtered set of tuples of file(s) for join, compute AND.
#                     and_index.remove(i)
#                     break
#                 elif conditions[i - 1][1] == '=' and not isinstance(conditions[i + 1], list):
#                     # TODO: (Join)if right block has filtered set of tuples of file(s) for join, compute AND.
#                     and_index.remove(i)
#                     break
#                 elif not isinstance(conditions[i - 1], list) and isinstance(conditions[i + 1], list):
#                     # TODO: left cases for right block, may be only: atrribute comparing attribute.
#                     and_index.remove(i)
#                     break
#                 elif isinstance(conditions[i - 1], list) and not isinstance(conditions[i + 1], list):
#                     # TODO: left cases for left block, may be only: atrribute comparing attribute.
#                     and_index.remove(i)
#                     break
#                 elif not isinstance(conditions[i - 1], list) or not isinstance(conditions[i + 1], list):
#                     # TODO: left block or right block is a set and this no need to check if has filtered set of tuples of file(s)
#                     and_index.remove(i)
#                     break
#                 elif isinstance(conditions[i - 1], list) and isinstance(conditions[i + 1], list):
#                     # TODO: left and right blocks are all complicate predicate, no way just implement it.
#                     and_index.remove(i)
#                     break
#
#         while or_index:
#             for i in or_index:
#                 if not isinstance(conditions[i - 1], list) and not isinstance(conditions[i + 1], list):
#                     # TODO:
#                     or_index.remove(i)
#                     break
#                 elif not isinstance(conditions[i - 1], list) and conditions[i + 1][1] == '=':
#                     # TODO:
#                     or_index.remove(i)
#                     break
#                 elif conditions[i - 1][1] == '=' and not isinstance(conditions[i + 1], list):
#                     # TODO:
#                     or_index.remove(i)
#                     break
#                 elif not isinstance(conditions[i - 1], list) and isinstance(conditions[i + 1], list):
#                     # TODO:
#                     or_index.remove(i)
#                     break
#                 elif isinstance(conditions[i - 1], list) and not isinstance(conditions[i + 1], list):
#                     # TODO:
#                     or_index.remove(i)
#                     break
#                 elif not isinstance(conditions[i - 1], list) or not isinstance(conditions[i + 1], list):
#                     # TODO: left block or right block is a set and this no need to check if has filtered set of tuples of file(s)
#                     or_index.remove(i)
#                     break
#                 elif isinstance(conditions[i - 1], list) and isinstance(conditions[i + 1], list):
#                     # TODO:
#                     or_index.remove(i)
#                     break


def parse_from(from_lst):
    for x in from_lst:
        tbl_path = join(db_dir, x[0])
        tbl_map[x[0]] = tbl_path
        if len(x) > 1:
            for i in range(1, len(x)):
                if x[i] in tbl_map.keys():
                    pass  # means error
                tbl_map[x[i]] = tbl_path
                if x[0] in alias_dict.keys():
                    alias_dict[x[0]].append(x[i])
                else:
                    alias_dict[x[0]] = [x[i]]
                    # alias_set.add(x[i])
        else:
            alias_dict[x[0]] = []
    for x in alias_dict.keys():
        if len(alias_dict[x]) == 0:
            df_map[x] = db_dfs[tbl_map[x]].copy()
        elif len(alias_dict[x]) == 1:
            df_map[x] = db_dfs[tbl_map[x]].copy()
            df_map[alias_dict[x][0]] = df_map[x]
            alias_cast_dict[alias_dict[x][0]] = x
        else:
            for y in alias_dict[x]:
                df_map[y] = db_dfs[tbl_map[x]].copy()
                df_rename_by_alias(df_map[y], y)


def df_rename_by_alias(my_df, prefix):
    old_columns = list(my_df.columns.values)
    new_columns = [prefix + "." + x for x in old_columns]
    my_df.columns = new_columns
    return my_df


def extract_ta(s):
    if '.' not in s:
        for x in df_map.keys():
            if s in df_map[x].columns:
                return x, s
    else:
        return s.split('.')[0], s.split('.')[1]


def get_key_by_alias(alias):
    for name in alias_dict.keys():
        if alias in alias_dict:
            return True, name
    return False, ""


def get_data(s):
    if is_quoted(s):
        s = s[1:-1]
    try:
        res = int(s)
        return res
    except ValueError:
        try:
            res = float(s)
            return res
        except ValueError:
            try:
                res = dateutil.parser.parse(s)
                return res
            except ValueError:
                return s


def is_date(s):
    try:
        dateutil.parser.parse(s)
        return True
    except ValueError:
        return False


def is_float(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def is_quoted(s):
    if (s[0] == "'" and s[-1] == "'") or (s[0] == '"' and s[-1] == '"'):
        return True
    return False


def get_set(n):
    x = set()
    for i in range(0, n + 1):
        x.add(i)
    return x


def df_print_all(x):
    pd.set_option('display.max_rows', len(x))
    output=x.to_string(index=False)
    print(output)
    pd.reset_option('display.max_rows')


if __name__ == "__main__":
    print("Program starts")
    tm1 = time.time()
    # read_files()
    # df = db_dfs["simpleDb\\movies.csv"]
    read_files("simpleDb")
    lst = ['director_facebook_likes', 'actor_2_facebook_likes', "aspect_ratio"]
    df = db_dfs["simpleDb\\movies.csv"]
    s_df = df[lst]
    print(s_df)
    tm2 = time.time()
    print(tm2 - tm1)
