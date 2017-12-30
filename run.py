import itertools
import pyparsing
import time
import itertools
# from pandas_query import exec_sql, read_files
from parseQuery import parseQ
from global_vars import *
import QueryManager
import IndexManager

DEBUG = True
OMIT_IMPORT = True
# OMIT_IMPORT = False

db_dir = "../yelpDb"


def main():
    global db_dir
    global index_manager
    need_import_db = True
    while True:
        if not OMIT_IMPORT:
            if need_import_db:
                print("Please import your database <import> + <database directory path>")
                input_line = input(">> ")
            else:
                input_line = input(">> ")
            if input_line == "":
                continue
            if input_line.lower() == "exit()":
                print("Bye!")
                break
            input_token = input_line.split(" ")
            if input_token[0] == "import":
                if len(input_token) != 2:
                    print("Please input only one database.")
                    continue
                else:
                    db_dir = input_line[1]
                    meta_path = db_dir + INDEX_META_PATH
                    index_manager = IndexManager.IndexManager(meta_path, db_dir)
                    index_manager.load_index()
                    need_import_db = False
                    continue
        else:
            need_import_db = False
            meta_path = db_dir + INDEX_META_PATH
            index_manager = IndexManager.IndexManager(meta_path, db_dir)
            index_manager.load_index()
            input_line = input(">> ")
            input_token = input_line.split(" ")
        # Database has been loaded
        if len(input_token) == 1:
            print("Please input something in your query statement.")
            continue
        else:  # Start a query
            query = ' '.join(input_token)
            print(query)
            # todo check if the table attr has conflict with keywords
            if " distinct " in query.lower():
                tmp_query = query.lower()
                start_index = tmp_query.find("distinct") + 8
                query = query[0:start_index] + "," + query[start_index:]
            if " on " in query.lower():
                tmp_query = query.lower()
                start_index = tmp_query.find("on")
                query = query[0:start_index] + "," + query[start_index:]
            elif " where " in query.lower():
                tmp_query = query.lower()
                start_index = tmp_query.find("where")
                query = query[0:start_index] + "," + query[start_index:]
            else:
                query = query + ","
            if DEBUG:
                print("modified query: ", query)
            if "select" not in query.lower() or "from" not in query.lower():
                print("Please follow the input format SELECT-FROM_WHERE.")
                continue
            try:
                parsed_query = parseQ(str(query))

                if "select" in query and parsed_query.columns == '' or "on" in query and parsed_query.joinon == '' or "where" in query and parsed_query.where == '':
                    # if "select" in query and parsed_query.columns == '' or "from" in query and parsed_query.where == '' or "on" in query and parsed_query.joinon == '' or "where" in query and parsed_query.where == '':
                    print("Please fill in SELECT-FROM-WHERE clauses with good contents.")
                    continue

                print(parsed_query.columns)
                print(parsed_query.tables)
                print(parsed_query.joinon)
                print(parsed_query.where)

                splited_where = simplify_where(parsed_query.where.asList())

                # splited_where = []
                # left_position = 0
                # where = parsed_query.where.asList()
                # for i, condition in enumerate(where):
                #     if condition == "or":
                #         splited_where.append(where[left_position: i])
                #         left_position = i + 1
                # splited_where.append(where[left_position:])

                queries = []
                for i, condition_set in enumerate(splited_where):
                    splited_query = {}
                    splited_query['selected'] = '*' if parsed_query.columns == '*' else parsed_query.columns.asList()
                    splited_query['table'] = parse_from(parsed_query.tables[:-1])
                    # Handle second
                    splited_query['join'] = [] if parsed_query.joinon == '' else parse_join(parsed_query.joinon.asList())
                    # Handle first
                    splited_query['index_process'] = []
                    # Handle third
                    splited_query['pandas_process'] = []
                    for condition in condition_set:
                        if condition != "and" and condition != "or":
                            if get_priority(condition) == 1:
                                splited_query['index_process'].append(condition)
                            else:
                                splited_query['pandas_process'].append(condition)

                    queries.append(splited_query)
                print(queries)

                # TODO: get query result from query set.
                #execute_query(queries)

            except pyparsing.ParseException as pe:
                print("Error:" + pe.msg)


def parse_join(join):
    res = []
    for op in join:
        if isinstance(op, list):
            res.append(op)

    return res


def simplify_where(where):
    final_splited_where = []

    splited_where = []
    left_position = 0
    for i, condition in enumerate(where):
        if condition == "or":
            splited_where.append(where[left_position: i])
            left_position = i + 1
    splited_where.append(where[left_position:])
    # print(splited_where)

    for sub_where in splited_where:
        and_conditions = []
        or_conditions = []
        for i in range(len(sub_where)):
            op = sub_where[i]
            if isinstance(op, list) and '(' in op and ')' in op:
                for j in range(len(op)):
                    sub_op = op[j]
                    if isinstance(sub_op, list):
                        or_conditions.append(sub_op)
            elif isinstance(op, list) and '(' not in op and ')' not in op:
                and_conditions.append(op)

        if len(or_conditions) > 0:
            for or_condition in or_conditions:
                temp = and_conditions + [or_condition]
                final_splited_where.append(temp)
        else:
            final_splited_where.append(sub_where)

    # print("-000000")
    # print(final_splited_where)
    return final_splited_where


def get_priority(condition):
    # e.g. ['not', 'a'] ['a']
    if len(condition) == 1 or len(condition) == 2:
        return 1
    # e.g. ['a', '=', '5.5'] ['a', 'like', "'something'"]
    if len(condition) == 3:
        right_val = condition[2]
        if right_val.replace('.', '', 1).isdigit() or right_val[0] == '"' and right_val[-1] == '"' or right_val[
            0] == "'" and right_val[-1] == "'":
            return 1
        else:
            return 2

    # e.g. ['a', '=', 'c', '-', '5']
    return 2


def execute_query(query_set):
    print("--------------------executing----------------------")
    # columns = query_set['columns']
    # tables = query_set['tables']
    # join_on = query_set['joinon']
    # conditions = query_set['conditions']
    start_time = time.time()
    # exec_sql(columns, tables, joinon, conditions, max_level)
    qm = QueryManager.QueryManager(db_dir, index_manager, query_set)
    qm.do_query()
    used_time = time.time() - start_time
    print("used_time", used_time)


def is_split(iterable, splitters):
    return [list(g) for k, g in itertools.groupby(iterable, lambda x: x in splitters) if not k]


def parse_from(tables):
    return is_split(tables, ("join",))


def parse_where(conditions, level, max_level):
    for i in range(len(conditions)):
        op = conditions[i]
        if isinstance(op, list) and '(' in op and ')' in op:
            conditions[i] = parse_where(op, level + 1, max_level)

    for i in range(len(conditions)):
        op = conditions[i]
        if isinstance(op, list) and len(op) > 3 and ('(' not in op and ')' not in op):
            conditions[i] = op[0:2] + [(op[2:])]
        if isinstance(op, str) and (op == 'and' or op == 'or' or op == 'not'):
            conditions[i] = "_" + str(level) + "_" + op

    max_level[0] = max(max_level[0], level)
    return conditions


def format_where(conditions, li):
    for op in conditions:
        if isinstance(op, list) and '(' in op and ')' in op:
            format_where(op, li)
        elif op == '(' or op == ')':
            continue
        else:
            li.append(op)


if __name__ == "__main__":
    main()
