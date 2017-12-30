# some common utils for the whole project
import statistics
from os.path import isdir, isfile, dirname, basename
from os import mkdir

import time

import pickle

import dateutil

from global_vars import *
import pandas as pd
from io import StringIO

# DEBUG = True
DEBUG = False


def get_field_length(my_list):
    """
    This method is used to test if a field's length is fixed, and return the estimated length for a field
    :param my_list: the list to analyse
    :return: is fixed? True: False; mode_length
    """
    if len(my_list) == 0:
        raise Exception("no data to compare")
    len_list = []
    for _x in my_list:
        len_list.append(len(_x[0]))
    length = len_list[0]
    if all(_x == length for _x in len_list):
        return True, length
    return False, statistics.mode(tuple(len_list))


def read_block(file_path, offset, size):
    """
    This method is used to retrieve some data from a file
    :param file_path: the file to read from
    :param offset: the pos to start read
    :param size: length to read
    :return: a string of read result (should be like a line of csv)
    """
    with open(file_path, 'rb') as file:
        file.seek(int(offset))
        result = file.read(int(size))
        result = result.decode("utf-8")
    # file.close()  # todo: if I want to read multiple lines? don't close immediately?
    return result


def convert_filename(filename, target_type, attr=None, level=1, create_dir=False, separator=SEPARATOR):
    """
    Used to convert a file to corresponding file
    :param level: index level
    :param separator:
    :param attr: only for making index file
    :param create_dir: if corresponding dir not exist, create it
    :param filename: file's name (must be in a full path)
    :param target_type: csv, index, address
    :return: the converted filename
    """
    file = filename.split('.')
    table_dir = dirname(filename)
    table_name = basename(file[-2].lstrip())
    if separator in table_name:
        table_name = table_name.split(separator)[0]
    if target_type == FILE_TYPE_CSV:
        target_dir = dirname(table_dir)
        file_path = target_dir + r"/" + table_name + FILE_TYPE_CSV
    else:
        if table_dir.endswith("/index") or table_dir.endswith("/tmp"):
            table_dir = dirname(table_dir)
        if target_type == FILE_TYPE_ADDRESS:
            target_dir = table_dir + ADDRESS_DIR
        elif target_type == FILE_TYPE_INDEX:
            target_dir = table_dir + INDEX_DIR
        else:
            raise Exception("type cannot be resolved")
        if not isdir(target_dir):
            if create_dir:
                mkdir(target_dir)
            else:
                raise Exception("Dir not found")
        if target_type == FILE_TYPE_ADDRESS:
            file_path = target_dir + table_name + FILE_TYPE_ADDRESS
        else:
            file_path = target_dir + table_name + separator + attr + separator + str(level) + FILE_TYPE_INDEX
    return file_path


def table2csv(table, db_path, suffix):
    """
    Return a valid csv file path corresponding to given table
    :param suffix:  should be csv
    :param table: table
    :param db_path: database's path
    :return:
    """
    if not isdir(db_path):
        raise Exception("database path error")
    db_path = str(db_path)
    if not db_path.endswith("/"):
        file_path = db_path + "/" + table + suffix
    else:
        file_path = db_path + table + suffix
    if not isfile(file_path):
        raise Exception("database path error")
    return file_path


def decode_index_file(filename, separator):
    """
    Get the file_path, attr, level information from the name
    :param separator:
    :param filename:
    :return: (file_path, attr, level)
    """
    file = filename.split('.')
    name = basename(file[-2].lstrip())
    fields = name.split(separator)
    return fields[0], fields[1], int(fields[2])


def fetch_record(file, row_id, line_size, separator):
    """
    This method is used to fetch a certain record
    :param separator: separator for the record
    :param line_size: which means the length of a record
    :param file: the file used to save metadata
    :param row_id: the ith record (1st record stays on line2,since header use 1 row)
    :return: a string contains record
    """
    address_file = convert_filename(file, FILE_TYPE_ADDRESS)
    offset = line_size * row_id + row_id
    address_data = read_block(address_file, offset, line_size)
    if separator not in address_data:
        print("why")
    csv_offset = int(address_data.split(separator)[1])
    offset = (line_size + 1) * (row_id + 1)
    address_data2 = read_block(address_file, offset, line_size)  # fixme: need to handle edge problem
    if separator not in address_data2:  # which means has come to the end
        csv_length = -1
    else:
        csv_length = int(address_data2.split(separator)[1]) - csv_offset - 1
    csv_data = read_block(file, csv_offset, csv_length)
    return csv_data


def rowid2df(csv_file, rowid_list, line_size, separator):
    """
    get a data_frame using given file and row ids
    :param separator: as usual
    :param line_size: as usual
    :param csv_file: the csv file to use
    :param rowid_list: the ids
    :return:
    """
    data = fetch_record(csv_file, 0, line_size, separator)  # this helps to get csv header
    for rowid in rowid_list:
        if DEBUG:
            tmp_data = fetch_record(csv_file, rowid, line_size, separator)
            print(tmp_data)
        data += fetch_record(csv_file, rowid, line_size, separator)
    return pd.read_csv(StringIO(data))


def obj2disk(filename, obj):
    with open(filename, 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)


def disk2obj(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def convert_type(x, expected_type):
    if type(x) == list:
        if expected_type == INTEGER:
            return [int(y) for y in x]
        elif expected_type == REAL:
            return [float(y) for y in x]
        elif expected_type == DATE:
            return [dateutil.parser.parse(y) for y in x]
        else:
            return x
    if expected_type == INTEGER:
        return int(x)
    elif expected_type == REAL:
        return float(x)
    elif expected_type == DATE:
        return dateutil.parser.parse(x)
    else:
        return str(x)


def extract_ta(name):
    """
    To split table and attribute
    :param name:
    :return:
    """
    if '.' not in name:
        return None, name
    else:
        return name.split('.')[0], name.split('.')[1]


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


def is_date(s):
    try:
        dateutil.parser.parse(s)
        return True
    except ValueError:
        return False


def extract_data(s):
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


def rename_attr(table, attr,separator):
    return table+separator+attr

if __name__ == "__main__":
    start = time.time()
    # print(convert_filename("./sdb/tmp/review.address", FILE_TYPE_CSV))
    # print(convert_filename("./sdb/index/review#review_id.index", FILE_TYPE_CSV))
    # print(convert_filename("./sdb/review.csv", FILE_TYPE_ADDRESS))
    # print(convert_filename("./sdb/tmp/review.address", FILE_TYPE_INDEX,"review_id",2))
    # print(decode_index_file("./sdb/index/review#review_id#2.index",SEPARATOR))
    # print(convert_filename("./sdb/index/review#review_id.index", FILE_TYPE_CSV))
    # print(fetch_record("./sdb/review.csv", 2, LINE_SIZE, SEPARATOR))
    # for x in range(10):
    #     print(fetch_record("./sdb/review.csv", x, LINE_SIZE, SEPARATOR))
    # my_list = []
    # for x in range(300):
    #     my_list.append(x)
    # print(rowid2df("../csv/review.csv", my_list, LINE_SIZE, SEPARATOR))
    # rowid2df("../csv/review.csv", my_list, LINE_SIZE, SEPARATOR)
    # print(rowid2df("./sdb/review.csv", my_list, LINE_SIZE, SEPARATOR))

    # test_dict={1:"wyy",2:"sss"}
    # obj2disk("wyy.index",test_dict)
    # retrieve_dict=disk2obj("wyy.index")
    # retrieve_dict[1]="wind"
    # print(table2csv("business", r"C:\Users\wyy\Documents\Projects\UIUC\cs411-track2\sdb", ".csv"))
    print(table2csv("business", r"../yelpDb", ".csv"))
    print(time.time() - start)
