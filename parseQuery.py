from pyparsing import CaselessLiteral, Word, delimitedList, Optional, \
    Combine, Group, alphas, nums, alphanums, Forward, oneOf, quotedString, \
    ZeroOrMore, Keyword


# define SQL tokens
def load_grammar():
    selectStmt = Forward()
    SELECT = Keyword("select", caseless=True)
    FROM = Keyword("from", caseless=True)
    WHERE = Keyword("where", caseless=True)
    join_ = Keyword("join", caseless=True)
    distinct_ = Keyword("distinct", caseless=True)

    ident = Word(alphas, alphanums + "_-$").setName("identifier")
    columnName = (delimitedList(ident, ".", combine=True)).setName("column name")
    columnNameList = Group(delimitedList(columnName))
    tableName = (delimitedList(ident, ".", combine=True)).setName("table name")
    tableNameList = tableName + Optional(ident) + ZeroOrMore(join_ + tableName + Optional(ident)) + ","

    joinExpression = Forward()
    whereExpression = Forward()
    on_ = Keyword("on", caseless=True)
    and_ = Keyword("and", caseless=True)
    or_ = Keyword("or", caseless=True)
    in_ = Keyword("in", caseless=True)
    not_ = Keyword("not", caseless=True)
    like_ = Keyword("like", caseless=True)

    E = CaselessLiteral("E")
    binop = oneOf("= != < > >= <=", caseless=True)
    arithop = oneOf("+ - * /", caseless=True)
    arithSign = Word("+-", exact=1)
    realNum = Combine(Optional(arithSign) + (Word(nums) + "." + Optional(Word(nums)) |
                                             ("." + Word(nums))) +
                      Optional(E + Optional(arithSign) + Word(nums)))
    intNum = Combine(Optional(arithSign) + Word(nums) +
                     Optional(E + Optional("+") + Word(nums)))

    algExpression = (columnName + arithop + (realNum | intNum)) | ((realNum | intNum) + arithop + columnName)
    columnRval = algExpression | realNum | intNum | quotedString | columnName  # need to add support for alg expressions
    joinCondition = Group(columnName + binop + columnName)
    joinExpression << joinCondition + ZeroOrMore((and_ | or_) + joinExpression)
    whereCondition = Group(
        (columnName + binop + columnRval) |
        ("(" + whereExpression + ")") |
        (not_ + columnName) |
        (columnName + like_ + quotedString) |
        (columnName)   # TODO: deal with wildcard
    )
    whereExpression << whereCondition + ZeroOrMore((and_ | or_) + whereExpression)

    # define the grammar
    selectStmt = (SELECT + ('*' | columnNameList)("columns") +
                  FROM + tableNameList("tables") +
                  Optional(on_ + "(" + joinExpression("joinon") + ")") +
                  Optional(WHERE + whereExpression("where")))
    return selectStmt


def parseQ(stmt):
    selectStmt = load_grammar()
    return selectStmt.parseString(stmt)
