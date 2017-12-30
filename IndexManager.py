# This class is used to record all index files to use
import time

from global_vars import *
import MultiIndex
import utils


class IndexManager(object):
    index_dict = {}  # key is (table, attr), value is a pointer to the corresponding MultiIndex

    def __init__(self, filename, db_path):
        self.record_filename = filename
        self.db_path = db_path

    def save_index(self):
        utils.obj2disk(self.record_filename, self.index_dict)

    def load_index(self):
        self.index_dict = utils.disk2obj(self.record_filename)

    def insert_index(self, table, attr, attr_type, short_sep, long_sep, block_size):
        csv_file = utils.table2csv(table, self.db_path, ".csv")
        index_obj = MultiIndex.MultiIndex(csv_file, attr, attr_type, short_sep, long_sep, block_size)
        index_obj.create_index()
        self.index_dict[(table, attr)] = index_obj

    def remove_index(self, table, attr):
        if (table, attr) in self.index_dict:
            self.index_dict.pop((table, attr))


if __name__ == "__main__":
    start = time.time()
    im = IndexManager("./index_meta", "../yelpDb")
    im.load_index()
    # im.insert_index("photos", "business_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("photos", "photo_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("photos", "caption", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # print("photos finished")
    # im.insert_index("review", "review_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("review", "user_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("review", "business_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("review", "date", DATE, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # print("review finished")
    # im.insert_index("business", "business_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "categories", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "city", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "latitude", REAL, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "longitude", REAL, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "name", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "postal_code", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "review_count", INTEGER, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # im.insert_index("business", "state", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # print("business finished")
    # im.insert_index("checkin", "business_id", TEXT, SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # print("checkin finished")
    # im.save_index()
    print(time.time()-start)
    print("wait")
