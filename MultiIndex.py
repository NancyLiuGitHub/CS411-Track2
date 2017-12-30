import time
from global_vars import *
import pandas as pd
import utils


class MultiIndex(object):
    """
    Currently, I use a multi-index with an easy approach.
    Forget B+Tree and ISAM, I just use the simplest index method.
    """
    def __init__(self, csv_file, attr, attr_type, short_sep, long_sep, block_size):
        """
        :param csv_file: should be the csv file to make index
        :param attr: the attribute to index
        :param attr_type: the attribute's type
        """
        self.csv_file = csv_file
        self.attr = attr
        self.attr_type = attr_type
        self.short_sep = short_sep
        self.long_sep = long_sep
        self.revised_block_size = block_size * 0.95
        self.block_size = block_size
        self.index_dict = {}
        self.max_level = 0
        # self.estimate_size=50

    def load_index_meta(self):
        index_meta_file = utils.convert_filename(self.csv_file, FILE_TYPE_INDEX, attr=self.attr, level=0)
        self.index_dict = utils.disk2obj(index_meta_file)
        self.max_level = len(self.index_dict)

    def create_index(self):
        upper_list, index_size = self.create_level_index(1)
        level = 2
        while index_size > self.block_size:
            upper_list, index_size = self.create_level_index(level, upper_list)
            level += 1
        self.max_level = level - 1
        index_meta_file = utils.convert_filename(self.csv_file, FILE_TYPE_INDEX, attr=self.attr, level=0)
        utils.obj2disk(index_meta_file, self.index_dict)

    def create_level_index(self, level, input_list=None):
        """
        Generate the index file, level 0 for meta file, level 1 for primary index
        Others are sparse indexes
        """
        index_filename = utils.convert_filename(self.csv_file, FILE_TYPE_INDEX, attr=self.attr, level=level,
                                                create_dir=True)
        if level in self.index_dict:
            raise Exception("Found used level, check your code")
        self.index_dict[level] = index_filename
        if level == 0:
            pass
        elif level == 1:
            # create primary index
            df = pd.read_csv(self.csv_file, encoding='utf-8')
            tmp_list = list(df[self.attr])
            column_list = []
            for i, item in enumerate(tmp_list):
                column_list.append([utils.convert_type(item,self.attr_type), i])
            sorted_column = sorted(column_list)
            return self.save_index(index_filename, sorted_column)
        else:
            return self.save_index(index_filename, input_list, True)

    def save_index(self, filename, column_list, has_length=False):
        """
        :param has_length: do we want to save the lenght of the index?
        :param filename: the file to save the indices
        :param column_list: the indices
        :return: a list of upper level list (i.e. save 2nd level, get 3rd level)
        """
        with open(filename, "w", encoding='utf-8') as wf:
            block = ""
            end_loc = 0
            new_block = True
            next_list = []
            data_incomplete = False

            i = 0
            # if i < len(column_list):
            #     last_content = column_list[0][0]
            #     last_pos = 0
            last_pos = 0
            while i < len(column_list):
                if new_block:
                    new_block = False
                    data_incomplete = True
                line = str(column_list[i][0]) + self.long_sep
                line += str(column_list[i][1])
                if has_length:
                    line += self.short_sep + str(column_list[i][2])
                else:
                    while i < len(column_list) - 1 and str(column_list[i + 1][0]) == str(column_list[i][0]):
                        # which means duplication
                        i += 1
                        line += self.short_sep + str(column_list[i][1])
                block += line + '\n'
                if len(block) > self.revised_block_size:
                    start_loc = end_loc
                    wf.write(block)
                    wf.flush()
                    end_loc = wf.tell()
                    index_size = end_loc - start_loc
                    # if i + 1 < len(column_list):
                    next_list.append([column_list[last_pos][0], start_loc, index_size])
                    last_pos = i + 1
                    block = ""
                    new_block = True
                    data_incomplete = False
                i += 1
            if data_incomplete:  # If we left some part to record
                start_loc = end_loc
                wf.write(block)
                wf.flush()
                end_loc = wf.tell()
                index_size = end_loc - start_loc
                next_list.append([column_list[last_pos][0], start_loc, index_size])
        return next_list, end_loc

    def look_up(self, content, equal=True, greater=False, smaller=False):
        """
        To look up wanted data
        :param content: the content we are looking for
        :param equal: we want equal to value
        :param greater: we want greater than the value
        :param smaller: we want smaller than the value
        :return: a list of row ids, if the answer is not found, []
        """
        index_level_list = sorted(self.index_dict.keys(), reverse=True)
        offset = 0
        data_size = -1
        # level_id=index_level_list[0]
        for level_id in index_level_list:
            data = utils.read_block(self.index_dict[level_id], offset, data_size)
            if level_id == 1:
                return self.look_up_data(data, content, equal=equal, greater=greater, smaller=smaller)
            else:
                may_exist, offset, data_size = self.look_up_index(data, content, equal=equal, greater=greater,
                                                                  smaller=smaller)

    def look_up_index(self, data, content, equal=True, greater=False, smaller=False):
        """
        Use binary search to get the data we want
        :return: a list of row ids or [may_exist, offset, data_size]
        """
        # first, we locate the content's position
        lines = data.splitlines()
        length = len(lines)
        low = int(0)
        high = int(length - 1)
        middle = int((high - low) / 2 + low)
        while low <= high:
            middle = int((high - low) / 2 + low)
            val_loc = lines[middle].split(self.long_sep)
            attr_value = val_loc[0]
            # Integer, Real, Text, Date, Boolean
            attr_value = utils.convert_type(attr_value, self.attr_type)
            if attr_value == content:
                break  # get the target block
            if attr_value < content:
                low = middle + 1
            else:
                high = middle - 1
        if low <= high:  # which means we "catch it" in this level
            # if compare == '<=' or compare == '>=' or compare == "=" or compare == ">":
            if equal or greater:
                # When compare is >, we cannot make sure the result, need further validation
                may_exist = True
                info = lines[middle].split(self.long_sep)[1]
            elif smaller:
                if middle == 0:
                    return [False, 0, 0]
                else:  # In fact, this is a special situation, we can determine which block to use directly
                    may_exist = True
                    info = lines[middle - 1].split(self.long_sep)[1]
            else:
                raise Exception("This situation should be resolved earlier")
            offset = info.split(self.short_sep)[0]
            data_length = info.split(self.short_sep)[1]
            return [may_exist, offset, data_length]
        else:  # Can't find the value in this level, but maybe we can roll down
            # if compare == '=' or compare == '<=' or compare == '>=' or compare == '>':
            if equal or greater:
                may_exist = True
            elif smaller:
                if high == -1:
                    may_exist = False
                else:
                    may_exist = True
            else:
                raise Exception("This situation should be resolved earlier")
            info = lines[high].split(self.long_sep)[1]
            offset = info.split(self.short_sep)[0]
            data_length = info.split(self.short_sep)[1]
            return [may_exist, offset, data_length]

    def look_up_data(self, data, content, equal=True, greater=False, smaller=False):
        lines = data.splitlines()
        length = len(lines)
        low = int(0)
        high = int(length - 1)
        middle = int((high - low) / 2 + low)
        while low <= high:
            middle = int((high - low) / 2 + low)
            val_loc = lines[middle].split(self.long_sep)
            attr_value = val_loc[0]
            # Integer, Real, Text, Date, Boolean
            attr_value = utils.convert_type(attr_value, self.attr_type)
            if attr_value == content:
                break  # get the target block
            if attr_value < content:
                low = middle + 1
            else:
                high = middle - 1
        result_list = []
        if equal and smaller:
            if low <= high:  # catch it
                for i in range(0, middle + 1):
                    info = lines[i].split(self.long_sep)[1]
                    result_list.append(info.split(self.short_sep))
                return result_list
            else:
                if high == -1:
                    return []
                else:
                    for i in range(0, high + 1):
                        info = lines[i].split(self.long_sep)[1]
                        result_list.append(info.split(self.short_sep))
                    return result_list
        elif equal and greater:
            if low <= high:
                for i in range(middle, len(lines)):
                    info = lines[i].split(self.long_sep)[1]
                    result_list.append(info.split(self.short_sep))
                return result_list
            else:
                if low == len(lines):
                    return []
                for i in range(low, len(lines)):
                    info = lines[i].split(self.long_sep)[1]
                    result_list.append(info.split(self.short_sep))
                return True, result_list
        elif equal:
            if low <= high:
                info = lines[middle].split(self.long_sep)[1]
                return info.split(self.short_sep)
            else:
                return []
        elif smaller:
            if low <= high:
                if middle == 0:
                    return []
                for i in range(0, middle):
                    info = lines[i].split(self.long_sep)[1]
                    result_list.append(info.split(self.short_sep))
                return result_list
            else:
                if high == -1:
                    return []
                else:
                    for i in range(0, high + 1):
                        info = lines[i].split(self.long_sep)[1]
                        result_list.append(info.split(self.short_sep))
                    return result_list
        elif greater:
            if low <= high:
                if middle == len(lines) - 1:
                    return []
                for i in range(middle + 1, len(lines)):
                    info = lines[i].split(self.long_sep)[1]
                    result_list.append(info.split(self.short_sep))
                return result_list
            else:
                if low == len(lines):
                    return []
                for i in range(low, len(lines)):
                    info = lines[i].split(self.long_sep)[1]
                    result_list.append(info.split(self.short_sep))
                return result_list
        else:
            raise Exception("This situation should be resolved earlier")


if __name__ == "__main__":
    start = time.time()
    print("app starts")
    # index = MultiIndex("sdb\\photos.csv", "business_id", "Text", SHORT_SEP, LONG_SEP, PAGE_SIZE)
    index = MultiIndex("sdb\\photos.csv", "business_id", "Text", SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # index.create_index()
    index.load_index_meta()
    result=index.look_up("0N2y8rNxbet6p4UIBWTOrw")
    # MultiIndex(r"../yelpDb/review.csv", "review_id", "Text", SHORT_SEP, LONG_SEP, PAGE_SIZE)
    # data=utils.read_block(utils.convert_filename("sdb\\photos.csv",FILE_TYPE_INDEX,"business_id",separator=SEPARATOR),0,3987)
    print(time.time() - start)
    print("wait")
