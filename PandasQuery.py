import pandas as pd
import time
from global_vars import *
import utils

DEBUG = True


class PandasQuery(object):
    data_frames = {}  # a dic used to map table to df
    alias_map = {}

    def __init__(self, db_path, separator):
        self.db_path = db_path
        self.separator = separator

    def add_table(self, table, data_frame, alias=None):
        """
        To record the table in data_frames
        :param table: the table name will be used later, must be unique
        :param data_frame: corresponding data frame
        :param alias: some alias for the table
        :return:
        """
        if alias is not None:
            for name in alias:
                self.alias_map[name] = table
        self.alias_map[table] = table
        self.data_frames[table] = data_frame

    def df_columns_rename(self, table):
        my_df = self.data_frames[table]
        old_columns = list(my_df.columns.values)
        new_columns = [table + self.separator + x for x in old_columns]
        my_df.columns = new_columns

    def do_join(self, table1, table2, attr_lst1, attr_lst2):
        df1 = self.data_frames[table1]
        df2 = self.data_frames[table2]
        return pd.DataFrame.merge(df1, df2, left_on=attr_lst1, right_on=attr_lst2, how="inner")

    @staticmethod
    def df_print_all(x):
        pd.set_option('display.max_rows', len(x))
        output = x.to_string(index=False)
        print(output)
        pd.reset_option('display.max_rows')

    def do_fix_where(self, my_df, attr, value, opr, return_set=True):
        if return_set:
            if opr.upper() == 'LIKE':
                return self.do_match(my_df, attr, value)
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
                return self.do_match(my_df, attr, value, False)
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

    @staticmethod
    def do_dynamic_where(my_df, my_attr, other_attr, opr, extra_value=None, extra_opr=None):
        if extra_value is not None and extra_opr is not None:
            tmp_col = TMP_COL + other_attr
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

    def do_where(self, my_df, attr, value, opr):
        tbl, attr = self.extract_ta(attr)
        # if tbl is None:
        #     pass
        # else:
        #     table = self.alias_map[tbl]
        if isinstance(value, list):
            return self.do_dynamic_where(my_df, attr, value[0], opr, value[2], value[1])
        elif utils.is_float(value) or utils.is_date(value) or utils.is_quoted(value):
            par = utils.extract_data(value)
            return self.do_fix_where(my_df, attr, par, opr)
        else:
            return self.do_dynamic_where(my_df, attr, value, opr)

    # @staticmethod
    def do_select(self, my_df, attr_lst):
        selected_attr_list = []
        if attr_lst[0] == "*":
            all_cols = list(my_df.columns.values)
            for x in all_cols:
                if x.startswith(TMP_COL):
                    continue
                selected_attr_list.append(x)
            return my_df[selected_attr_list]
        for attr in attr_lst:
            tbl, att = self.extract_ta(attr)
            # if tbl in alias_cast_dict:
            #     attr = att
            selected_attr_list.append(att) # fixme: may need more consideration
        selected_df = my_df[list(selected_attr_list)]
        return selected_df

    @staticmethod
    def get_df_by_set(my_df, my_set):
        """
        select a df by a set of row ids
        """
        return my_df.iloc[list(my_set), :]

    @staticmethod
    def do_match(my_df, attr, string, return_set=True):
        # my_str="r'.*?"+string+".*'"
        my_str = string.split('%')
        if len(my_str) == 1:
            if return_set:
                return set(my_df[my_df[attr] == my_str].index.values.tolist())
            else:
                return my_df[my_df[attr] == my_str]
        if string[0] != '%':
            string = '^' + string
        if string[len(string) - 1] != '%':
            string = string + '$'
        to_query = string.replace("%", "[\s\S]*")
        tmp = my_df[my_df[attr].str.contains(to_query, na=False)]
        if return_set:
            return set(tmp.index.values.tolist())
        else:
            return tmp

    def extract_ta(self, statement):
        if '.' not in statement:
            for x in self.data_frames.keys():
                if statement in self.data_frames[x].columns:
                    return x, statement
        else:
            return statement.split('.')[0], statement.split('.')[1]


if __name__ == "__main__":
    print("Program starts")
    tm1 = time.time()

    tm2 = time.time()
    print(tm2 - tm1)
