import time
from os.path import isfile, join
from os import listdir
from global_vars import *
import utils
import MultiIndex


class PreProcess(object):
    """
    This class provides some methods to preprocess the file
    """
    tmp_dir = None
    line_size = 0
    separator = None

    def __init__(self, line_size, separator):
        self.line_size = line_size
        self.separator = separator

    def create_all_metadata(self, dir_name):
        files = [dir_name + f for f in listdir(dir_name) if isfile(join(dir_name, f))]
        for f in files:
            self.create_metadata(f)

    def create_metadata(self, filename):
        """
        This method helps to create a file contains row_number->strat_pos, row_length
        :param filename:
        :return: None, but will create a file with modified info
        """
        file_path = utils.convert_filename(filename, utils.FILE_TYPE_ADDRESS, create_dir=True)
        address_file = open(file_path, 'w')
        line_start = 0
        meta_str = ""
        # with codecs.open(filename, "r", encoding="utf-8") as f:
        with open(filename, "r", encoding="utf-8") as f:
            need_pair = False
            tmp_str = self.separator + str(line_start) + "\n"
            tmp_str = tmp_str.zfill(self.line_size)
            for line in f:
                line_start += len(line.encode("utf8"))
                if need_pair:
                    if line.count('"') % 2 == 1:
                        need_pair = False
                        line_start += 1
                        tmp_str = self.separator + str(line_start) + "\n"
                        tmp_str = tmp_str.zfill(self.line_size)
                else:
                    meta_str += tmp_str
                    if line.count('"') % 2 == 1:
                        need_pair = True
                    else:
                        line_start += 1
                        tmp_str = self.separator + str(line_start) + "\n"
                        tmp_str = tmp_str.zfill(self.line_size)
        address_file.write(meta_str)
        address_file.flush()
        address_file.close()


if __name__ == "__main__":
    print("app starts")
    start = time.time()
    pp = PreProcess(LINE_SIZE, SEPARATOR)
    # pp.create_metadata(r"../yelpDb/review.csv")
    # pp.create_metadata(r"./sdb/review.csv")
    pp.create_all_metadata(r"../yelpDb/")
    # index_list=[]
    # index=
    # index_list.append(MultiIndex("sdb\\photos.csv", "business_id", "Text", SHORT_SEP, LONG_SEP, PAGE_SIZE))
    # pp.create_all_metadata(r"./sdb/")
    # pp.create_metadata(r"../csv/review.csv")
    print(time.time() - start)

    print("wait")
