# lexicalAnalysis.py
# 重新实现的 SQL 词法分析器，输入从 stdin 读取直到 EOF

sql_keywords = {
    "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "TABLE", "INDEX", "DROP", "ALTER", "ADD", "AND", "OR", "NOT", "NULL", "AS", "JOIN",
    "INNER", "LEFT", "RIGHT", "FULL", "OUTER",
    "ON", "ORDER", "BY", "GROUP", "HAVING", "DISTINCT", "LIMIT", "OFFSET",     "SHOW", "EXPLAIN",
    "TABLES", "COLUMNS", "VIEWS",
    # 聚合函数
    "COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT",
    # 高级条件
    "IN", "BETWEEN", "LIKE", "IS", "EXISTS",
    # 排序
    "ASC", "DESC",
    # 表约束关键字
    "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT", "AUTO_INCREMENT",
    # 触发器关键字
    "TRIGGER", "BEFORE", "AFTER", "INSTEAD", "OF", "FOR", "EACH", "ROW", "WHEN", "BEGIN", "END", "IF", "THEN", "ELSE", "NEW", "OLD", "SIGNAL", "SQLSTATE",
    # 时间函数
    "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
    # 常见数据类型也视为关键字
    "INT", "INTEGER", "SMALLINT", "BIGINT", "TINYINT", "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC",
    "CHAR", "VARCHAR", "TEXT", "DATE", "TIME", "TIMESTAMP", "BOOLEAN", "BLOB"
}

# 缓冲区和位置
prog = ""
pos = 0
line = 1
col = 0  # 列从0开始，表示下一个字符的列索引

# 扫描得到的当前 token 信息（由 scanner 填充）
tok_type = None
tok_value = None
tok_line = 0
tok_col = 0

# 辅助函数
def peek_char():
    global prog, pos
    if pos >= len(prog):
        return '\0'
    return prog[pos]

def next_char():
    global prog, pos, line, col
    if pos >= len(prog):
        return '\0'
    ch = prog[pos]
    pos += 1
    if ch == '\n':
        line += 1
        col = 0
    else:
        col += 1
    return ch

# 将当前列作为 token 起始列（注意：调用时 col 表示下一个要读的字符的列索引）
def current_pos():
    return line, col

# 判断关键字
def is_keyword(s):
    return s.upper() in sql_keywords

# 扫描下一个 token
def scanner():
    global prog, pos, line, col
    global tok_type, tok_value, tok_line, tok_col

    tok_type = None
    tok_value = None

    # 跳过空格和制表，不跳过换行（因为换行要维护行号）
    while True:
        c = peek_char()
        if c == ' ' or c == '\t' or c == '\r':
            next_char()
            continue
        break

    if pos >= len(prog) or peek_char() == '\0':
        tok_type = 'EOF'
        return

    # 记录 token 起始位置
    tok_line = line
    tok_col = col
    tok_line, tok_col = current_pos()

    c = peek_char()

    # 处理换行：直接消费并继续扫描下一个 token
    if c == '\n':
        next_char()
        scanner()
        return

    # 注释：多行 /* ... */
    if c == '/' and pos + 1 < len(prog) and prog[pos + 1] == '*':
        tok_line, tok_col = current_pos()
        comment = ''
        comment += next_char()  # consume '/'
        comment += next_char()  # consume '*'
        while True:
            ch = next_char()
            if ch == '\0':
                break
            comment += ch
            if ch == '*' and peek_char() == '/':
                comment += next_char()  # consume '/'
                break
        tok_type = 'COMMENT'
        tok_value = comment
        tok_line = tok_line
        tok_col = tok_col
        return

    # 注释：单行 -- 到行尾
    if c == '-' and pos + 1 < len(prog) and prog[pos + 1] == '-':
        tok_line, tok_col = current_pos()
        comment = ''
        comment += next_char()  # '-'
        comment += next_char()  # '-'
        while True:
            ch = peek_char()
            if ch == '\0' or ch == '\n':
                break
            comment += next_char()
        tok_type = 'COMMENT'
        tok_value = comment
        return

    # 标识符或关键字���以字��或下划线开头）
    if c.isalpha() or c == '_':
        tok_line, tok_col = current_pos()
        ident = ''
        while True:
            ch = peek_char()
            if ch.isalnum() or ch == '_':
                ident += next_char()
            else:
                break
        tok_value = ident
        tok_type = 'KEYWORD' if is_keyword(ident) else 'ID'
        return

    # 数字（������或浮点）
    if c.isdigit():
        tok_line, tok_col = current_pos()
        num = ''
        is_float = False
        while True:
            ch = peek_char()
            if ch.isdigit():
                num += next_char()
            else:
                break
        if peek_char() == '.' and (pos + 1 < len(prog) and prog[pos + 1].isdigit()):
            is_float = True
            num += next_char()  # consume '.'
            while True:
                ch = peek_char()
                if ch.isdigit():
                    num += next_char()
                else:
                    break
        tok_type = 'NUMBER'
        tok_value = num
        return

    # 字符串：单引号包围，支持用两个连续单引号表示单引号字符
    if c == "'":
        tok_line, tok_col = current_pos()
        s = ''
        next_char()  # consume opening '
        while True:
            ch = next_char()
            if ch == '\0':
                break
            if ch == "'":
                # 如果下一个也是单引号，则是转义，继续读取并保留两个单引号
                if peek_char() == "'":
                    s += next_char()
                    continue
                else:
                    break
            else:
                s += ch
        tok_type = 'STRING'
        tok_value = s
        return

    # 字符串：双引号包围
    if c == '"':
        tok_line, tok_col = current_pos()
        s = ''
        next_char()  # consume opening "
        while True:
            ch = next_char()
            if ch == '\0':
                break
            if ch == '"':
                # 如果下一个也是双引号，则是转义，继续读取并保留两个双引号
                if peek_char() == '"':
                    s += next_char()
                    continue
                else:
                    break
            else:
                s += ch
        tok_type = 'STRING'
        tok_value = s
        return

    # 运算符和界符
    # 支持复合运算符： <= >= <> != == ||
    two_char_ops = {'<>', '<=', '>=', '!=', '==', '||'}
    single = peek_char()
    nxt = prog[pos + 1] if pos + 1 < len(prog) else '\0'
    cand2 = single + nxt
    if cand2 in two_char_ops:
        tok_line, tok_col = current_pos()
        tok_value = cand2
        tok_type = 'OP'
        next_char()
        next_char()
        return

    # 单字符运算符或界符
    single_ops = set('+-*/%<>!=.,;()')
    ch = peek_char()
    if ch in single_ops:
        tok_line, tok_col = current_pos()
        ch = next_char()
        # 分号、逗号和括号作为单独的token类型
        if ch == ';':
            tok_type = ';'
            tok_value = ch
        elif ch == ',':
            tok_type = ','  # 逗号作为单独的token类型
            tok_value = ch
        elif ch in {'(', ')'}:
            tok_type = ch  # 直接使用括号作为token类型
            tok_value = ch
        else:
            tok_type = 'OP'
            tok_value = ch
        return

    # 其它未知字符，返回 UNKNOWN 并消费
    tok_line, tok_col = current_pos()
    tok_value = next_char()
    tok_type = 'UNKNOWN'
    return


# 把脚本改为可重用模块：提供 tokenize(text) 接口，返回四元组列表
# 原先的 __main__ 交互式读取已移除，以便其他代码导入并调用 tokenize。

def tokenize(text):
    """对给定的 SQL 文本进行词法分析，返回一个由四元组组成的列表：
    (type, value, line, column)
    不包含 EOF。
    """
    global prog, pos, line, col
    # 初始化缓冲区和位置
    if text is None:
        text = ''
    prog = text + '\0'
    pos = 0
    line = 1
    col = 0

    tokens = []
    while True:
        scanner()
        if tok_type == 'EOF' or tok_type is None:
            break
        tokens.append((tok_type, tok_value, tok_line, tok_col))
    return tokens
