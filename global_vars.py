# This file contains all global variables used in the whole project
SEPARATOR = "#"
LONG_SEP = "_#_"
SHORT_SEP = "|"
LINE_SIZE = 12  # use this attribute to adjust the line length of an index
ADDRESS_DIR = r"/tmp/"
INDEX_DIR = r"/index/"

FILE_TYPE_ADDRESS = ".address"
FILE_TYPE_INDEX = ".index"
FILE_TYPE_CSV = ".csv"

KB = 1024
MB = KB * 1024
SAMPLE_LEN = 200
PAGE_SIZE = 4 * KB  # Based on the cluster size(4K)
REVISED_PG_SIZE = PAGE_SIZE * 0.95  # Reserve 5% of the space for overflow

INTEGER = "Integer"
REAL = "Real"
DATE = "Date"
TEXT = "Text"
BOOLEAN = "Boolean"
UNKNOWN = 'Unknown'

TMP_COL = "tmp##__"

OPERATOR = "operator"
OPERATOR_AND = "AND"
OPERATOR_OR = "OR"
OPERATOR_NOT = "NOT"
OPERATOR_PLUS = "+"
OPERATOR_DIVIDE = "/"
OPERATOR_MINUS = "-"
OPERATOR_MULTIPLY = "*"
OPERAND = "operand"

INDEX_META_PATH = INDEX_DIR + "index_meta"
