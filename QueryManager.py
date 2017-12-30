# This class is used to execute query based on index (index first, then pandas)
import utils
from global_vars import *
from PandasQuery import PandasQuery
from MultiIndex import MultiIndex
import IndexManager
import time
import pandas as pd
import pandas_utils

DEBUG = True


class QueryManager(object):
    """
    One QueryManager corresponds to one sql sentence, it will generate the best sequence for it,
    and save corresponding table-->data frame for pandas's use
    The total logic: use "or" to separate the query, each query will be represented by a df when single
    conditions are used up
    """
    alias2table = {}
    table2alias = {}
    alias2csv = {}
    alias2set = {}  # an alias(table) should correspond to a set or rows
    alias2df = {}
    df2union = []  # here is a list of dataframe to union as the result

    def __init__(self, db_dir, index_manager, query_dict_list):
        self.db_dir = db_dir
        self.index_manager = index_manager  # index manager should be instanced only once, at the beginning
        self.query_dict_list = query_dict_list
        self.select_list = self.query_dict_list[0]['selected']
        self.from_list = self.query_dict_list[0]['table']
        self.join_list = self.query_dict_list[0]['join']
        self.parse_from()
        print("wait")

    def do_union(self):
        pass

    def do_join(self, i):
        """
        Used the ith element in self.alias2set to join and get a dataframe
        :param i: dict:i
        :return: a dataframe with all joined data
        """
        alias2df = {}
        from_dict = {}
        for tbl in self.alias2set[i].keys():
            rowid_list = self.alias2set[tbl]
            alias2df[tbl] = (utils.rowid2df(self.alias2table[tbl], list(map(int, rowid_list)), PAGE_SIZE, SEPARATOR))
            if self.alias2df[tbl] in self.table2alias and len(self.table2alias[self.alias2df[tbl]]) > 1:
                pandas_utils.df_columns_rename(alias2df[tbl], tbl, SEPARATOR)
        for condition in self.join_list:
            tb1 = utils.extract_ta(condition[0])
            tb2 = utils.extract_ta(condition[2])
            if tb1 < tb2:
                if (tb1, tb2) not in from_dict:
                    from_dict[(tb1, tb2)] = [[condition[0], condition[2]]]
                else:
                    from_dict[(tb1, tb2)].append([condition[0], condition[2]])
            else:
                if (tb1, tb2) not in from_dict:
                    from_dict[(tb2, tb1)] = [[condition[0], condition[2]]]
                else:
                    from_dict[(tb2, tb1)].append([condition[0], condition[2]])
        used_tbls = set()
        all_df = pd.DataFrame
        for x in from_dict.keys():
            attr_lst1 = [y[0] for y in from_dict[x]]
            attr_lst2 = [y[1] for y in from_dict[x]]
            if self.alias2table[x[0]] in self.table2alias.keys() and len(self.table2alias[self.alias2table[x[0]]]) > 1:
                attr_lst1 = [utils.rename_attr(x[0], y[1], SEPARATOR) for y in attr_lst1]
            if self.alias2table[x[1]] in self.table2alias.keys() and len(self.table2alias[self.alias2table[x[1]]]) > 1:
                attr_lst2 = [utils.rename_attr(x[1], y[1], SEPARATOR) for y in attr_lst2]

            if x[0] in used_tbls:
                used_tbls.add(x[1])
                all_df = self.join_2dfs(all_df, self.alias2df[x[1]], attr_lst1, attr_lst2)
            elif x[1] in used_tbls:
                used_tbls.add(x[0])
                all_df = self.join_2dfs(self.alias2df[x[0]], all_df, attr_lst1, attr_lst2)
            else:
                used_tbls.add(x[0])
                used_tbls.add(x[1])
                all_df = self.join_2dfs(self.alias2df[x[0]], self.alias2df[x[1]], attr_lst1, attr_lst2)

    @staticmethod
    def join_2dfs(df1, df2, attr_lst1, attr_lst2):
        return pd.DataFrame.merge(df1, df2, left_on=attr_lst1, right_on=attr_lst2, how="inner")

    def parse_from(self):
        """
        This method deals with from, detach them, save in dicts
        """
        # appeared_table = set()
        for item in self.from_list:
            if len(item) == 1:
                # which means no alias for this table
                self.alias2table[item[0]] = item[0]
            elif len(item) == 2:
                self.alias2table[item[1]] = item[0]
                if item[0] not in self.table2alias:
                    self.table2alias[item[0]] = [item[1]]
                else:
                    self.table2alias[item[0]].append(item[1])
        for item in self.alias2table.keys():
            self.alias2csv[item] = utils.table2csv(self.alias2table[item], self.db_dir, ".csv")

    def do_pandas_where(self, data_frame, conditions):
        pass

    def do_index_where(self, condition):
        """
        Only deal with where who has single fix condition, has the highest priority
        :param condition:
        :return: a list of row ids
        """
        # if len(condition) != 1:
        #     raise Exception("For index query, only supports for one condition")
        # if isinstance(condition[0][2], list):
        #     raise Exception("Only support fixed condition for index now")

        content = condition[2]
        table, attr = self.extract_original_ta(condition[0])
        if (table, attr) not in self.index_manager.index_dict:
            raise Exception("TODO: what to do if the condition is not in index")
        elif condition[1] == ">":
            return self.index_manager.index_dict[(table, attr)].look_up(content, equal=False, larger=True)
        elif condition[1] == ">=":
            return self.index_manager.index_dict[(table, attr)].look_up(content, equal=True, larger=True)
        elif condition[1] == "<":
            return self.index_manager.index_dict[(table, attr)].look_up(content, equal=False, smaller=True)
        elif condition[1] == "<=":
            return self.index_manager.index_dict[(table, attr)].look_up(content, equal=True, smaller=True)
        elif condition[1] == "=":
            return self.index_manager.index_dict[(table, attr)].look_up(content, equal=True)
        elif condition[1] == "!=":
            return self.index_manager.index_dict[(table, attr)].look_up(content, equal=False, larger=True, smaller=True)
        else:
            raise Exception("command cannot resolved")

    def extract_original_ta(self, statement):
        tbl = statement.split('.')[0]
        att = statement.split('.')[1]
        return self.alias2table[tbl], att

    def do_query(self):
        print("start query")
        for i, query_dict in enumerate(self.query_dict_list):
            self.alias2df[i] = {}
            self.alias2set[i] = {}
            for to_index in query_dict['index_process']:
                # self.alias2set[i][utils.extract_ta(to_index)[0]] = self.do_index_where(to_index)
                self.alias2set[i][utils.extract_ta(to_index)[0]] = self.do_index_where(to_index)

            joined_df = self.do_join(i)
            self.df2union.append(joined_df)
            for to_pandas in query_dict['pandas_process']:
                self.do_pandas_where(joined_df, to_pandas)


if __name__ == "__main__":
    im = IndexManager.IndexManager("./index_meta", "../yelpDb")
    im.load_index()
    qm = QueryManager("./", im, "", [['review', 'R1'], ['review', 'R2'], ['business', 'B']], "", "")
    qm.parse_from()
    result = qm.do_index_where([['B.business_id', '=', "0N2y8rNxbet6p4UIBWTOrw"]])
    print(result)
