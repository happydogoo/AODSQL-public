# -*- coding: utf-8 -*-
"""
全新的SQL语法分析器 - 简洁、可靠、易维护
支持AODSQL项目所需的核心SQL功能
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from .lexicalAnalysis import tokenize
from .ast_nodes import (
    AggregateFunction, Identifier, BinaryExpr, Literal, ColumnDefinition,
    ForeignKeyConstraint, PrimaryKeyConstraint, UniqueConstraint,
    CreateTriggerStatement, DropTriggerStatement, ShowTriggersStatement,
    OldNewReference, WhenCondition, IfStatement, SignalStatement,
    DeclareCursorStatement, OpenCursorStatement, FetchCursorStatement, CloseCursorStatement,
    SelectStatement, SubqueryExpression
)


class NewSyntaxAnalyzer:
    """全新的SQL语法分析器"""
    
    def __init__(self):
        self.tokens = []
        self.current = 0
        self.errors = []
    
    def build_ast_from_tokens(self, tokens):
        """从token流构建AST"""
        self.tokens = tokens
        self.current = 0
        self.errors = []
        
        try:
            ast = self.parse_program()
            if self.errors:
                raise SyntaxError(f"语法错误: {'; '.join(self.errors)}")
            return ast
        except Exception as e:
            raise SyntaxError(f"语法分析失败: {str(e)}")
    
    def parse_query_from_tokens(self, tokens):
        """从token流解析查询（不包含分号）"""
        self.tokens = tokens
        self.current = 0
        self.errors = []
        
        try:
            query = self.parse_query()
            if self.errors:
                raise SyntaxError(f"语法错误: {'; '.join(self.errors)}")
            return query
        except Exception as e:
            raise SyntaxError(f"语法分析失败: {str(e)}")
    
    def parse_program(self):
        """解析程序: Query ;"""
        query = self.parse_query()
        
        # 改进：分号是可选的，添加错误恢复机制
        if self.match(';'):
            self.advance()
        else:
            # 错误恢复：尝试跳过到分号
            while not self.is_at_end() and not self.match(';'):
                self.advance()
            
            if self.match(';'):
                self.advance()
            else:
                # 如果确实没有分号，记录警告但继续执行
                self.errors.append("警告：语句缺少分号")
        
        return Program(query)
    
    def parse_query(self):
        """解析查询语句"""
        if self.match('CREATE'):
            return self.parse_create_statement()
        elif self.match('INSERT'):
            return self.parse_insert()
        elif self.match('SELECT'):
            return self.parse_select()
        elif self.match('UPDATE'):
            return self.parse_update()
        elif self.match('DELETE'):
            return self.parse_delete()
        elif self.match('DROP'):

            return self.parse_drop_statement()
        elif self.match('ALTER'):
            return self.parse_alter_statement()

        elif self.match('SHOW'):
            return self.parse_show()
        elif self.match('EXPLAIN'):
            return self.parse_explain()
        elif self.match('DECLARE'):
            return self.parse_declare_cursor()
        elif self.match('OPEN'):
            return self.parse_open_cursor()
        elif self.match('FETCH'):
            return self.parse_fetch_cursor()
        elif self.match('CLOSE'):
            return self.parse_close_cursor()
        elif self.match('BEGIN'):
            return self.parse_begin_transaction()
        elif self.match('COMMIT'):
            return self.parse_commit_transaction()
        elif self.match('ROLLBACK'):
            return self.parse_rollback_transaction()
        else:
            token = self.peek()
            if isinstance(token, tuple):
                self.error(f"期望 CREATE, INSERT, SELECT, UPDATE, DELETE, DROP, ALTER, SHOW, EXPLAIN, DECLARE, OPEN, FETCH 或 CLOSE，得到 {token[0]}")
            else:
                self.error(f"期望 CREATE, INSERT, SELECT, UPDATE, DELETE, DROP, ALTER, SHOW, EXPLAIN, DECLARE, OPEN, FETCH 或 CLOSE，得到 {token.type}")
    
    def parse_create_statement(self):
        """解析CREATE语句"""
        self.consume('CREATE', '期望 CREATE')
        
        if self.match('TABLE'):
            return self.parse_create_table()
        elif self.match('INDEX'):
            return self.parse_create_index()
        elif self.match('VIEW'):
            return self.parse_create_view()
        elif self.match('TRIGGER'):
            return self.parse_create_trigger()
        else:
            self.error(f"期望 TABLE, INDEX, VIEW 或 TRIGGER，得到 {self.peek()}")
    
    def parse_create_table(self):
        """解析CREATE TABLE语句"""
        self.consume('TABLE', '期望 TABLE')
        table_name = self.get_token_value(self.consume_id('期望表名'))
        self.consume('(', '期望 (')
        
        # 解析列定义和表级约束
        columns = []
        constraints = []
        
        # 解析第一个定义
        if not self.match(')'):
            definition = self.parse_column_or_constraint_definition()
            if isinstance(definition, ColumnDefinition):
                columns.append(definition)
            else:
                constraints.append(definition)
        
        # 解析后续的定义
        while self.match(','):
            self.advance()  # 消费逗号
            definition = self.parse_column_or_constraint_definition()
            if isinstance(definition, ColumnDefinition):
                columns.append(definition)
            else:
                constraints.append(definition)
        
        self.consume(')', '期望 )')
        return CreateTable(table_name, columns, constraints)
    
    def parse_create_trigger(self):
        """解析CREATE TRIGGER语句"""
        self.consume('TRIGGER', '期望 TRIGGER')
        trigger_name_token = self.consume_id('期望触发器名')
        trigger_name = Identifier(trigger_name_token, self.get_token_value(trigger_name_token))
        
        # 解析触发时机
        timing = self.parse_trigger_timing()
        
        # 解析触发事件
        events = self.parse_trigger_events()
        
        self.consume('ON', '期望 ON')
        table_name_token = self.consume_id('期望表名')
        table_name = Identifier(table_name_token, self.get_token_value(table_name_token))
        
        # 解析FOR EACH ROW
        is_row_level = self.parse_for_each_row()
        
        # 解析WHEN条件（可选）
        when_condition = self.parse_when_condition()
        
        # 解析触发器主体
        trigger_body = self.parse_trigger_body()
        
        return CreateTriggerStatement(
            trigger_name=trigger_name,
            table_name=table_name,
            timing=timing,
            events=events,
            is_row_level=is_row_level,
            when_condition=when_condition,
            trigger_body=trigger_body
        )
    
    def parse_trigger_timing(self):
        """解析触发时机：BEFORE, AFTER, INSTEAD OF"""
        if self.match('BEFORE'):
            self.advance()
            return 'BEFORE'
        elif self.match('AFTER'):
            self.advance()
            return 'AFTER'
        elif self.match('INSTEAD'):
            self.advance()
            self.consume('OF', '期望 OF')
            return 'INSTEAD OF'
        else:
            self.error('期望 BEFORE, AFTER 或 INSTEAD OF')
    
    def parse_trigger_events(self):
        """解析触发事件：INSERT, UPDATE, DELETE"""
        events = []
        
        if self.match('INSERT'):
            self.advance()
            events.append('INSERT')
        elif self.match('UPDATE'):
            self.advance()
            events.append('UPDATE')
        elif self.match('DELETE'):
            self.advance()
            events.append('DELETE')
        else:
            self.error('期望 INSERT, UPDATE 或 DELETE')
        
        # 支持多个事件（用OR连接）
        while self.match('OR'):
            self.advance()
            if self.match('INSERT'):
                self.advance()
                events.append('INSERT')
            elif self.match('UPDATE'):
                self.advance()
                events.append('UPDATE')
            elif self.match('DELETE'):
                self.advance()
                events.append('DELETE')
            else:
                self.error('期望 INSERT, UPDATE 或 DELETE')
        
        return events
    
    def parse_for_each_row(self):
        """解析FOR EACH ROW"""
        if self.match('FOR'):
            self.advance()
            self.consume('EACH', '期望 EACH')
            self.consume('ROW', '期望 ROW')
            return True
        return False
    
    def parse_when_condition(self):
        """解析WHEN条件（可选）"""
        if self.match('WHEN'):
            self.advance()
            condition = self.parse_condition()
            return WhenCondition(condition=condition)
        return None
    
    def parse_trigger_body(self):
        """解析触发器主体：BEGIN...END块"""
        self.consume('BEGIN', '期望 BEGIN')
        
        statements = []
        while not self.match('END') and not self.is_at_end():
            # 跳过分号
            if self.match(';'):
                self.advance()
                continue
            
            statement = self.parse_trigger_statement()
            statements.append(statement)
            
            # 如果下一个token是分号，跳过它
            if self.match(';'):
                self.advance()
        
        self.consume('END', '期望 END')
        return statements
    
    def parse_trigger_statement(self):
        """解析触发器主体中的语句"""
        if self.match('IF'):
            return self.parse_if_statement()
        elif self.match('UPDATE'):
            return self.parse_update()
        elif self.match('INSERT'):
            return self.parse_insert()
        elif self.match('DELETE'):
            return self.parse_delete()
        elif self.match('SELECT'):
            return self.parse_trigger_select()
        elif self.match('SIGNAL'):
            return self.parse_signal_statement()
        else:
            # 尝试解析为表达式语句（用于字符串连接等）
            try:
                return self.parse_expression()
            except:
                self.error(f'触发器主体中不支持的语句: {self.peek()}')
    
    def parse_if_statement(self):
        """解析IF语句"""
        self.consume('IF', '期望 IF')
        condition = self.parse_expression()
        self.consume('THEN', '期望 THEN')
        
        then_statements = []
        while not self.match('ELSE') and not self.match('END') and not self.is_at_end():
            statement = self.parse_trigger_statement()
            then_statements.append(statement)
        
        else_statements = []
        if self.match('ELSE'):
            self.advance()
            while not self.match('END') and not self.is_at_end():
                statement = self.parse_trigger_statement()
                else_statements.append(statement)
        
        self.consume('END', '期望 END')
        self.consume('IF', '期望 IF')
        
        return IfStatement(
            condition=condition,
            then_statements=then_statements,
            else_statements=else_statements
        )
    
    def parse_signal_statement(self):
        """解析SIGNAL语句"""
        self.consume('SIGNAL', '期望 SIGNAL')
        self.consume('SQLSTATE', '期望 SQLSTATE')
        
        # SQLSTATE值可以是字符串或标识符
        if self.match('STRING'):
            sqlstate = self.get_token_value(self.advance())
        else:
            sqlstate = self.get_token_value(self.consume_id('期望SQLSTATE值'))
        
        # 解析可选的 SET MESSAGE_TEXT
        message = ""
        if self.match('SET'):
            self.advance()  # 消费 SET
            self.consume('MESSAGE_TEXT', '期望 MESSAGE_TEXT')
            self.consume('=', '期望 =')
            if self.match('STRING'):
                message = self.get_token_value(self.advance())
            else:
                message = self.get_token_value(self.consume_id('期望错误消息'))
        
        return SignalStatement(sqlstate=sqlstate, message=message)
    
    def parse_trigger_select(self):
        """解析触发器中的SELECT语句（不需要FROM子句）"""
        self.consume('SELECT', '期望 SELECT')
        
        columns = self.parse_select_list()
        
        # 在触发器中，SELECT语句可能没有FROM子句
        if self.match('FROM'):
            self.advance()  # 消费 FROM
            table_ref = self.parse_table_reference()
            
            # 解析JOIN子句（可选）
            joins = []
            while self.match('JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL', 'OUTER'):
                join = self.parse_join()
                joins.append(join)
            
            # 解析WHERE子句（可选）
            where_condition = None
            if self.match('WHERE'):
                where_condition = self.parse_where_clause()
            
            # 解析GROUP BY子句（可选）
            group_by = None
            if self.match('GROUP'):
                group_by = self.parse_group_by_clause()
            
            # 解析HAVING子句（可选）
            having_condition = None
            if self.match('HAVING'):
                having_condition = self.parse_having_clause()
            
            # 解析ORDER BY子句（可选）
            order_by = None
            if self.match('ORDER'):
                order_by = self.parse_order_by_clause()
            
            return SelectStatement(
                select_list=columns,
                from_table=table_ref,
                joins=joins,
                where_clause=where_condition
            )
        else:
            # 没有FROM子句的SELECT语句（如 SELECT 'test'）
            # 创建一个虚拟的表引用
            dummy_table = Identifier(('ID', 'DUAL', 0, 0), 'DUAL')
            return SelectStatement(
                select_list=columns,
                from_table=dummy_table,
                joins=[],
                where_clause=None
            )
    
    def parse_column_or_constraint_definition(self):
        """根据下一个token判断是解析列定义还是表约束"""
        if self.match('FOREIGN'):
            return self.parse_foreign_key_constraint()
        elif self.match('PRIMARY'):
            return self.parse_primary_key_constraint()
        elif self.match('UNIQUE'):
            return self.parse_unique_constraint()
        else:
            # 默认是列定义
            return self.parse_column_definition()
    
    def parse_foreign_key_constraint(self):
        """解析 FOREIGN KEY 约束"""
        self.consume('FOREIGN', '期望 FOREIGN')
        self.consume('KEY', '期望 KEY')
        self.consume('(', '期望 (')
        column_name_token = self.consume_id('期望列名')
        column_name = Identifier(column_name_token, self.get_token_value(column_name_token))
        self.consume(')', '期望 )')
        
        self.consume('REFERENCES', '期望 REFERENCES')
        ref_table_token = self.consume_id('期望引用的表名')
        ref_table_name = Identifier(ref_table_token, self.get_token_value(ref_table_token))
        self.consume('(', '期望 (')
        ref_column_token = self.consume_id('期望引用的列名')
        ref_column_name = Identifier(ref_column_token, self.get_token_value(ref_column_token))
        self.consume(')', '期望 )')
        
        return ForeignKeyConstraint(column_name, ref_table_name, ref_column_name)
    
    def parse_primary_key_constraint(self):
        """解析表级 PRIMARY KEY 约束"""
        self.consume('PRIMARY', '期望 PRIMARY')
        self.consume('KEY', '期望 KEY')
        self.consume('(', '期望 (')
        
        column_names = []
        column_token = self.consume_id('期望列名')
        column_names.append(Identifier(column_token, self.get_token_value(column_token)))
        
        while self.match(','):
            self.advance()  # 消费逗号
            column_token = self.consume_id('期望列名')
            column_names.append(Identifier(column_token, self.get_token_value(column_token)))
        
        self.consume(')', '期望 )')
        return PrimaryKeyConstraint(column_names)
    
    def parse_unique_constraint(self):
        """解析 UNIQUE 约束"""
        self.consume('UNIQUE', '期望 UNIQUE')
        self.consume('(', '期望 (')
        
        column_names = []
        column_token = self.consume_id('期望列名')
        column_names.append(Identifier(column_token, self.get_token_value(column_token)))
        
        while self.match(','):
            self.advance()  # 消费逗号
            column_token = self.consume_id('期望列名')
            column_names.append(Identifier(column_token, self.get_token_value(column_token)))
        
        self.consume(')', '期望 )')
        return UniqueConstraint(column_names)
    
    def parse_create_index(self):
        """解析CREATE INDEX语句，支持复合索引"""
        self.consume('INDEX', '期望 INDEX')
        index_name = self.get_token_value(self.consume_id('期望索引名'))
        self.consume('ON', '期望 ON')
        table_name = self.get_token_value(self.consume_id('期望表名'))
        self.consume('(', '期望 (')
        
        # 解析列名列表，支持复合索引
        columns = []
        columns.append(self.get_token_value(self.consume_id('期望列名')))
        
        while self.match(','):
            self.advance()  # 消费逗号
            columns.append(self.get_token_value(self.consume_id('期望列名')))
        
        self.consume(')', '期望 )')
        
        # 如果只有一列，传递字符串；多列传递列表
        column_spec = columns[0] if len(columns) == 1 else columns
        return CreateIndex(index_name, table_name, column_spec)
    
    def parse_create_view(self):
        """解析CREATE VIEW语句"""
        self.consume('VIEW', '期望 VIEW')
        view_name = self.get_token_value(self.consume_id('期望视图名'))
        self.consume('AS', '期望 AS')
        
        # 解析视图的SELECT语句
        select_statement = self.parse_select()
        return CreateView(view_name, select_statement)
    
    def parse_column_list(self):
        """解析列定义列表"""
        columns = []
        columns.append(self.parse_column_definition())
        
        while self.match(','):
            self.advance()  # 消费逗号
            columns.append(self.parse_column_definition())
        
        return columns
    
    def parse_column_definition(self):
        """解析列定义"""
        column_name_token = self.consume_id('期望列名')
        column_name = Identifier(column_name_token, self.get_token_value(column_name_token))
        data_type = self.parse_data_type()
        
        # 解析约束
        constraints = []
        while not self.is_at_end() and not self.match(')') and not self.match(','):
            if self.match('PRIMARY'):
                self.advance()  # 消费 PRIMARY
                self.consume('KEY', '期望 KEY')
                constraints.append('PRIMARY KEY')
            elif self.match('NOT'):
                self.advance()  # 消费 NOT
                self.consume('NULL', '期望 NULL')
                constraints.append('NOT NULL')
            elif self.match('UNIQUE'):
                self.advance()  # 消费 UNIQUE
                constraints.append('UNIQUE')
            elif self.match('DEFAULT'):
                self.advance()  # 消费 DEFAULT
                default_value = self.parse_default_value()
                constraints.append(f'DEFAULT {default_value}')
            elif self.match('AUTO_INCREMENT'):
                self.advance()  # 消费 AUTO_INCREMENT
                constraints.append('AUTO_INCREMENT')
            elif self.match('CHECK'):
                self.advance()  # 消费 CHECK
                check_constraint = self.parse_check_constraint()
                constraints.append(check_constraint)
            else:
                break
        
        return ColumnDefinition(column_name, data_type, constraints)
    
    def parse_default_value(self):
        """解析默认值"""
        if self.is_at_end():
            self.error('期望默认值')
        
        token = self.peek()
        if isinstance(token, tuple):
            if token[0] == 'NUMBER':
                return self.get_token_value(self.consume_num('期望默认值'))
            elif token[0] == 'STRING':
                return self.get_token_value(self.consume_string('期望默认值'))
            elif token[0] == 'ID':
                return self.get_token_value(self.consume_id('期望默认值'))
            elif token[0] == 'KEYWORD':
                # 处理关键字作为默认值（如CURRENT_TIMESTAMP）
                return self.get_token_value(self.advance())
            else:
                self.error('期望默认值')
        else:
            self.error('期望默认值')
    
    def parse_check_constraint(self):
        """解析CHECK约束"""
        self.consume('(', '期望 (')
        
        # 解析条件表达式
        condition = self.parse_condition()
        
        self.consume(')', '期望 )')
        
        # 返回CHECK约束的字符串表示
        return f'CHECK ({self._condition_to_string(condition)})'
    
    def _condition_to_string(self, condition):
        """将条件表达式转换为字符串"""
        if hasattr(condition, 'left') and hasattr(condition, 'right'):
            # 二元表达式
            left_str = self._condition_to_string(condition.left)
            right_str = self._condition_to_string(condition.right)
            operator = getattr(condition, 'operator', getattr(condition, 'op', '='))
            return f"{left_str} {operator} {right_str}"
        elif hasattr(condition, 'name'):
            # 标识符
            return condition.name
        elif hasattr(condition, 'value'):
            # 字面量
            return str(condition.value)
        else:
            return str(condition)
    
    def parse_begin_transaction(self):
        """解析BEGIN TRANSACTION语句"""
        self.advance()  # 消费 BEGIN
        if self.match('TRANSACTION'):
            self.advance()  # 消费 TRANSACTION
        return BeginTransactionStatement()
    
    def parse_commit_transaction(self):
        """解析COMMIT TRANSACTION语句"""
        self.advance()  # 消费 COMMIT
        return CommitTransactionStatement()
    
    def parse_rollback_transaction(self):
        """解析ROLLBACK TRANSACTION语句"""
        self.advance()  # 消费 ROLLBACK
        return RollbackTransactionStatement()
    
    def parse_data_type(self):
        """解析数据类型"""
        if self.match('INT'):
            self.advance()
            return DataType('INT')
        elif self.match('VARCHAR'):
            self.advance()
            if self.match('('):
                self.advance()  # 消费 (
                size = self.get_token_value(self.consume_num('期望数字'))
                self.consume(')', '期望 )')
                return DataType('VARCHAR', size)
            else:
                return DataType('VARCHAR')
        elif self.match('DECIMAL'):
            self.advance()
            if self.match('('):
                self.advance()  # 消费 (
                precision = self.get_token_value(self.consume_num('期望精度'))
                self.consume(',', '期望 ,')
                scale = self.get_token_value(self.consume_num('期望小数位数'))
                self.consume(')', '期望 )')
                return DataType('DECIMAL', precision, scale)
            else:
                return DataType('FLOAT')
        elif self.match('DATE'):
            self.advance()
            return DataType('DATE')
        elif self.match('TIME'):
            self.advance()
            return DataType('TIME')
        elif self.match('TIMESTAMP'):
            self.advance()
            return DataType('TIMESTAMP')
        elif self.match('BOOLEAN'):
            self.advance()
            return DataType('BOOLEAN')
        elif self.match('TEXT'):
            self.advance()
            return DataType('TEXT')
        elif self.match('FLOAT'):
            self.advance()
            return DataType('FLOAT')
        elif self.match('DOUBLE'):
            self.advance()
            return DataType('FLOAT')
        elif self.match('REAL'):
            self.advance()
            return DataType('FLOAT')        
        else:
            token = self.peek()
            if isinstance(token, tuple):
                self.error(f"期望数据类型，得到 {token[0]}")
            else:
                self.error(f"期望数据类型，得到 {token.type}")
    
    def parse_insert(self):
        """解析INSERT语句 - 支持多行VALUES"""
        try:
            self.consume('INSERT', '期望 INSERT')
            self.consume('INTO', '期望 INTO')
            
            table_name = self.get_token_value(self.consume_id('期望表名'))
            
            # 解析列名列表（可选）
            columns = None
            if self.match('('):
                self.advance()  # 消费 (
                columns = self.parse_column_name_list()
                self.consume(')', '期望 )')
            
            self.consume('VALUES', '期望 VALUES')
            
            # 解析多行VALUES
            values_list = self.parse_multiple_value_lists()
            
            return Insert(table_name, values_list, columns)
        except Exception as e:
            # 提供更详细的错误信息
            current_token = self.peek() if not self.is_at_end() else None
            error_msg = f"INSERT语句解析失败: {e}"
            if current_token:
                if isinstance(current_token, tuple):
                    error_msg += f" (当前token: {current_token[0]} '{current_token[1]}')"
                else:
                    error_msg += f" (当前token: {current_token.type} '{current_token.literal}')"
            raise Exception(error_msg)
    
    def parse_column_name_list(self):
        """解析列名列表"""
        columns = []
        columns.append(self.get_token_value(self.consume_id('期望列名')))
        
        while self.match(','):
            self.advance()  # 消费逗号
            columns.append(self.get_token_value(self.consume_id('期望列名')))
        
        return columns
    
    def parse_value_list(self):
        """解析值列表"""
        values = []
        values.append(self.parse_value())
        
        while self.match(','):
            self.advance()  # 消费逗号
            values.append(self.parse_value())
        
        return values
    
    def parse_multiple_value_lists(self):
        """解析多行VALUES列表"""
        values_list = []
        
        # 解析第一行VALUES
        self.consume('(', '期望 (')
        values = self.parse_value_list()
        self.consume(')', '期望 )')
        values_list.append(values)
        
        # 解析后续的VALUES行
        while self.match(','):
            self.advance()  # 消费逗号
            self.consume('(', '期望 (')
            values = self.parse_value_list()
            self.consume(')', '期望 )')
            values_list.append(values)
        
        return values_list
    
    def parse_value(self):
        """解析值"""
        if self.is_at_end():
            self.error("期望值，得到 EOF")
        
        token = self.peek()
        if isinstance(token, tuple):
            if token[0] == 'NUMBER':
                token = self.advance()
                return Value('NUMBER', self.get_token_value(token))
            elif token[0] == 'STRING':
                token = self.advance()
                return Value('STRING', self.get_token_value(token))
            elif token[0] == 'ID' or token[0] == 'KEYWORD':
                # 处理标识符和关键字（如NEW.student_id）
                return self.parse_expression()
            else:
                self.error(f"期望值，得到 {token[0]}")
        else:
            if token.type == 'NUMBER':
                token = self.advance()
                return Value('NUMBER', self.get_token_value(token))
            elif token.type == 'STRING':
                token = self.advance()
                return Value('STRING', self.get_token_value(token))
            elif token.type == 'ID':
                # 处理标识符（如NEW.student_id）
                return self.parse_expression()
            else:
                self.error(f"期望值，得到 {token.type}")
    
    def parse_select(self):
        """解析SELECT语句"""
        self.consume('SELECT', '期望 SELECT')
        
        columns = self.parse_select_list()
        self.consume('FROM', '期望 FROM')
        
        # 解析表引用（可能是单个表或多个表的JOIN）
        table_ref = self.parse_table_reference()
        
        # 解析JOIN子句（可选）
        joins = []
        while self.is_join_keyword():
            join = self.parse_join()
            joins.append(join)
        
        where_clause = None
        if self.match('WHERE'):
            where_clause = self.parse_where_clause()
        
        group_by = None
        if self.match('GROUP'):
            group_by = self.parse_group_by()
        
        having_clause = None
        if self.match('HAVING'):
            having_clause = self.parse_having_clause()
        
        order_by = None
        if self.match('ORDER'):
            order_by = self.parse_order_by()
        
        limit = None
        if self.match('LIMIT'):
            limit = self.parse_limit()

        # 提取表名或子查询引用
        if hasattr(table_ref, 'tables'):
            # 多表引用，直接传递对象
            table_name = table_ref
        elif hasattr(table_ref, 'table_name'):
            table_name = table_ref.table_name
        elif hasattr(table_ref, 'subquery'):
            # 子查询引用，直接传递对象
            table_name = table_ref
        else:
            table_name = str(table_ref)
        
        return Select(columns, table_name, where_clause, order_by, joins, group_by, having_clause, limit)

    
    def parse_table_reference(self):
        """解析表引用或子查询，支持多表查询（逗号分隔）"""
        
        # 解析第一个表引用
        first_table = self._parse_single_table_reference()
        
        # 检查是否有逗号，表示多表查询
        if self.match(','):
            self.advance()  # 消费逗号
            # 解析剩余的表引用
            tables = [first_table]
            while True:
                table = self._parse_single_table_reference()
                tables.append(table)
                
                if self.match(','):
                    self.advance()  # 消费逗号
                else:
                    break
            
            # 返回多表引用
            return MultiTableReference(tables)
        else:
            # 单个表引用
            return first_table
    
    def _parse_single_table_reference(self):
        """解析单个表引用或子查询"""
        # 检查是否是子查询
        if self.match('('):
            self.advance()  # 消费 (
            
            if self.match('SELECT'):
                # 解析子查询
                subquery = self.parse_subquery()
                self.consume(')', '期望 )')
                
                # 子查询可以有别名，检查是否有AS关键字
                alias = None
                if self.match('AS'):
                    self.advance()
                    alias = self.get_token_value(self.consume_id('期望别名'))
                elif self.match('ID') and not self.match('WHERE') and not self.match('GROUP') and not self.match('ORDER'):
                    # 没有AS关键字，直接是别名（但要确保不是其他关键字）
                    alias = self.get_token_value(self.consume_id('期望别名'))
                
                return SubqueryReference(subquery, alias)
            else:
                self.error('FROM ( 后面期望 SELECT')
        
        # 原有解析表名的逻辑
        table_name = self.get_token_value(self.consume_id('期望表名'))
        
        # 检查是否有别名
        alias = None
        if self.match('AS'):
            self.advance()
            alias = self.get_token_value(self.consume_id('期望别名'))
        elif self.match('ID') and not self.match('ON') and not self.match(','):
            # 没有AS关键字，直接是别名（但要确保不是ON关键字或逗号）
            alias = self.get_token_value(self.consume_id('期望别名'))
        
        return TableReference(table_name, alias)
    
    def parse_join(self):
        """解析JOIN子句"""
        join_type = 'INNER'  # 默认内连接
        
        # 解析JOIN类型
        if self.match('INNER'):
            self.advance()
            self.consume('JOIN', '期望 JOIN')
            join_type = 'INNER'
        elif self.match('LEFT'):
            self.advance()
            if self.match('OUTER'):
                self.advance()
                join_type = 'LEFT OUTER'
            else:
                join_type = 'LEFT'
            self.consume('JOIN', '期望 JOIN')
        elif self.match('RIGHT'):
            self.advance()
            if self.match('OUTER'):
                self.advance()
                join_type = 'RIGHT OUTER'
            else:
                join_type = 'RIGHT'
            self.consume('JOIN', '期望 JOIN')
        elif self.match('FULL'):
            self.advance()
            if self.match('OUTER'):
                self.advance()
                join_type = 'FULL OUTER'
            else:
                join_type = 'FULL'
            self.consume('JOIN', '期望 JOIN')
        else:
            self.consume('JOIN', '期望 JOIN')
        
        # 解析右表名
        right_table_name = self.get_token_value(self.consume_id('期望表名'))
        
        # 检查是否有别名
        right_alias = None
        if self.match('ID'):
            # 检查下一个token是否是ON
            next_token = self.tokens[self.current + 1] if self.current + 1 < len(self.tokens) else None
            if next_token and (next_token[0] == 'KEYWORD' or next_token[0] == 'ID') and next_token[1].upper() == 'ON':
                # 下一个token是ON关键字，说明当前token是别名
                right_alias = self.get_token_value(self.consume_id('期望别名'))
        
        right_table = TableReference(right_table_name, right_alias)
        
        # 解析ON条件
        if not self.match('ON'):
            self.error(f"期望 ON，得到 {self.peek()}")
        self.advance()  # 跳过ON
        condition = self.parse_join_condition()
        
        return Join(join_type, right_table, condition)
    
    def parse_join_condition(self):
        """解析JOIN条件"""
        # 解析左表列
        left_table = self.get_token_value(self.consume_id('期望表名或别名'))
        self.consume('.', '期望 .')
        left_column = self.get_token_value(self.consume_id('期望列名'))
        
        # 解析操作符
        operator = '='
        if self.match('='):
            self.advance()
            operator = '='
        elif self.match('!='):
            self.advance()
            operator = '!='
        elif self.match('<>'):
            self.advance()
            operator = '<>'
        else:
            self.error(f"期望 =, != 或 <>，得到 {self.peek()}")
        
        # 解析右表列
        right_table = self.get_token_value(self.consume_id('期望表名或别名'))
        self.consume('.', '期望 .')
        right_column = self.get_token_value(self.consume_id('期望列名'))
        
        return JoinCondition(left_table, left_column, operator, right_table, right_column)
    
    def parse_select_list(self):
        """解析SELECT列表"""
        if self.match('*'):
            self.advance()
            return ['*']
        
        columns = []
        columns.append(self.parse_select_item())
        
        while self.match(','):
            self.advance()  # 消费逗号
            columns.append(self.parse_select_item())
        
        return columns
    
    def parse_select_item(self):
        """解析SELECT项目（列名、聚合函数或表达式）"""
        # 检查是否是聚合函数
        if self.is_aggregate_function():
            return self.parse_aggregate_function()
        else:
            # 解析表达式（包括字面量、列名、字符串连接等）
            expression = self.parse_expression()
            
            # 检查是否有别名
            alias = None
            if self.match('AS'):
                self.advance()
                alias = self.get_token_value(self.consume_id('期望别名'))
            elif self.match('ID') and not self.match('FROM') and not self.match('GROUP') and not self.match('ORDER'):
                # 没有AS关键字，直接是别名（但要确保不是其他关键字）
                alias = self.get_token_value(self.consume_id('期望别名'))
            
            # 如果有别名，返回一个包含别名的对象
            if alias:
                return ColumnWithAlias(expression, alias)
            else:
                return expression
    

    def parse_column_reference(self):
        """解析列引用（可能是 table_alias.column_name 格式）"""
        # 解析第一个标识符
        if not self.match('ID'):
            self.error('期望表名或列名')
        
        table_or_column_token = self.advance()
        table_or_column = self.get_token_value(table_or_column_token)
        
        # 检查是否有点号，表示是 table_alias.column_name 格式
        if self.match('.'):
            self.advance()  # 消费点号
            if not self.match('ID'):
                self.error('期望列名')
            column_token = self.advance()
            column_name = self.get_token_value(column_token)
            return f"{table_or_column}.{column_name}"
        else:
            # 只是列名
            return table_or_column
    

    def is_aggregate_function(self):
        """检查当前token是否是聚合函数"""
        if self.is_at_end():
            return False
        
        token = self.peek()
        if isinstance(token, tuple):
            token_type = token[0]
            token_value = token[1]
        else:
            token_type = token.type
            token_value = token.value
        
        aggregate_functions = {'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'GROUP_CONCAT'}
        return token_type == 'KEYWORD' and token_value in aggregate_functions
    
    def parse_expression(self):
        """解析表达式（支持算术运算和字符串连接）"""
        # 解析第一个操作数
        left = self.parse_primary()
        
        # 检查是否有运算符
        while self.match('+', '-', '*', '/', '%', '||'):
            operator = self.advance()
            right = self.parse_primary()
            left = BinaryExpr(left, operator, right)
        
        return left
    
    def parse_primary(self):
        """解析基本表达式（列名、数字、字符串等）"""
        if self.match('('):
            self.advance()  # 消费左括号
            
            # 检查是否是子查询
            if self.check('SELECT'):
                # 解析子查询
                subquery = self.parse_select()
                self.consume(')', '期望 )')
                return SubqueryExpression(subquery)
            else:
                # 普通括号表达式
                expr = self.parse_expression()
                self.consume(')', '期望 )')
                return expr
        elif self.match('ID'):
            # 使用 parse_column_reference 来处理表别名列名（如 o.order_id）
            column_ref = self.parse_column_reference()
            # 创建一个临时的 token 来表示列引用
            temp_token = ('ID', column_ref, 0, 0)
            return Identifier(temp_token, column_ref)
        elif self.match('OLD') or self.match('NEW'):
            # 处理 OLD.column 或 NEW.column 引用
            ref_type = self.get_token_value(self.advance())
            self.consume('.', '期望 .')
            column_token = self.consume_id('期望列名')
            column_name = Identifier(column_token, self.get_token_value(column_token))
            return OldNewReference(reference_type=ref_type, column_name=column_name)
        elif self.match('NUMBER'):
            token = self.advance()
            return Literal(token, self.get_token_value(token))
        elif self.match('STRING'):
            token = self.advance()
            return Literal(token, self.get_token_value(token))
        else:
            self.error(f"期望表达式，但遇到 {self.peek()}")
    
    def parse_aggregate_function(self):
        """解析聚合函数"""
        # 获取函数名
        function_token = self.advance()
        function_name = self.get_token_value(function_token)
        
        # 消费左括号
        self.consume('(', '期望 (')
        
        # 解析参数
        argument = None
        distinct = False
        
        if self.match('DISTINCT'):
            self.advance()  # 消费DISTINCT
            distinct = True
            col_token = self.advance()
            argument = Identifier(col_token, self.get_token_value(col_token))
        elif self.match('*'):
            self.advance()  # 消费*
            # COUNT(*) 的情况，argument保持None
        else:
            # 解析表达式（可能是列名或算术表达式）
            argument = self.parse_expression()
        
        # 消费右括号
        self.consume(')', '期望 )')
        
        # 检查是否有别名
        alias = None
        if self.match('AS'):
            self.advance()
            alias = self.get_token_value(self.consume_id('期望别名'))
        elif self.match('ID') and not self.match('FROM') and not self.match('GROUP') and not self.match('ORDER'):
            # 没有AS关键字，直接是别名（但要确保不是其他关键字）
            alias = self.get_token_value(self.consume_id('期望别名'))
        
        return AggregateFunction(function_name, argument, distinct, alias)
    
    def parse_where_clause(self):
        """解析WHERE子句"""
        self.consume('WHERE', '期望 WHERE')
        return self.parse_condition()
    
    def parse_condition(self):
        """解析条件（支持括号和OR/AND操作符）"""
        return self.parse_or_condition()
    
    def parse_or_condition(self):
        """解析OR条件"""
        left = self.parse_and_condition()
        
        while self.match('OR'):
            self.advance()  # 消费OR
            right = self.parse_and_condition()
            left = OrCondition(left, right)
        
        return left
    
    def parse_and_condition(self):
        """解析AND条件"""
        left = self.parse_primary_condition()
        
        while self.match('AND'):
            self.advance()  # 消费AND
            right = self.parse_primary_condition()
            left = AndCondition(left, right)
        
        return left
    
    def parse_primary_condition(self):
        """解析基本条件（支持括号）"""
        # 检查是否是EXISTS条件
        if self.match('EXISTS'):
            self.advance()  # 消费EXISTS
            self.consume('(', '期望 (')
            subquery = self.parse_select()
            self.consume(')', '期望 )')
            return ExistsCondition(subquery)
        
        # 检查是否是括号表达式
        if self.match('('):
            self.advance()  # 消费左括号
            condition = self.parse_condition()
            self.consume(')', '期望 )')
            return condition
        
        # 解析基本条件：表达式 操作符 表达式
        left = self.parse_expression()  # 修改：使用 parse_expression 来支持复杂表达式
        
        # 检查是否是BETWEEN语法
        if self.match('BETWEEN'):
            self.advance()  # 消费BETWEEN
            value1 = self.parse_expression()  # 修改：使用 parse_expression
            self.consume('AND', '期望 AND')
            value2 = self.parse_expression()  # 修改：使用 parse_expression
            return BetweenCondition(left, value1, value2)
        elif self.match('IN'):
            # 处理IN操作符
            self.advance()  # 消费IN
            self.consume('(', '期望 (')
            
            # 检查是否是子查询
            if self.check('SELECT'):
                subquery = self.parse_select()
                self.consume(')', '期望 )')
                return InCondition(left, subquery)
            else:
                # 值列表
                values = []
                if not self.match(')'):
                    values.append(self.parse_expression())
                    while self.match(','):
                        self.advance()  # 消费逗号
                        values.append(self.parse_expression())
                self.consume(')', '期望 )')
                return InCondition(left, values)
        else:
            # 普通操作符语法
            operator = self.parse_operator()
            right = self.parse_expression()  # 修改：使用 parse_expression
            return Condition(left, operator, right)
    
    def parse_operator(self):
        """解析操作符"""
        if self.match('='):
            self.advance()
            return '='
        elif self.match('>'):
            self.advance()
            return '>'
        elif self.match('<'):
            self.advance()
            return '<'
        elif self.match('>='):
            self.advance()
            return '>='
        elif self.match('<='):
            self.advance()
            return '<='
        elif self.match('!='):
            self.advance()
            return '!='
        elif self.match('LIKE'):
            self.advance()
            return 'LIKE'
        elif self.match('IN'):
            self.advance()
            return 'IN'
        else:
            token = self.peek()
            if isinstance(token, tuple):
                self.error(f"期望操作符，得到 {token[0]}")
            else:
                self.error(f"期望操作符，得到 {token.type}")
    
    def parse_order_by(self):
        """解析ORDER BY子句"""
        self.consume('ORDER', '期望 ORDER')
        self.consume('BY', '期望 BY')
        
        # 解析多个排序列
        order_columns = []
        
        while True:
            # 解析列名
            column = self.get_token_value(self.consume_id('期望列名'))
            direction = 'ASC'
            
            # 解析排序方向
            if self.match('DESC'):
                self.advance()
                direction = 'DESC'
            elif self.match('ASC'):
                self.advance()
                direction = 'ASC'
            
            order_columns.append(OrderBy(column, direction))
            
            # 检查是否还有更多列
            if self.match(','):
                self.advance()  # 消费逗号
                continue
            else:
                break
        
        # 如果只有一个列，返回单个OrderBy对象以保持兼容性
        if len(order_columns) == 1:
            return order_columns[0]
        else:
            # 返回OrderByList对象
            return OrderByList(order_columns)
    
    def parse_limit(self):
        """解析LIMIT子句"""
        self.consume('LIMIT', '期望 LIMIT')
        
        # 解析限制数量
        limit_token = self.consume_num('期望数字')
        limit_count = int(self.get_token_value(limit_token))
        
        return Limit(limit_count)
    
    def parse_group_by(self):
        """解析GROUP BY子句"""
        self.consume('GROUP', '期望 GROUP')
        self.consume('BY', '期望 BY')
        
        # 解析多个分组列
        group_columns = []
        
        while True:
            # 解析列名（支持表别名列名）
            column = self.parse_column_reference()
            group_columns.append(column)
            
            # 检查是否还有更多列
            if self.match(','):
                self.advance()  # 消费逗号
                continue
            else:
                break
        
        return GroupBy(group_columns)
    

    def parse_having_clause(self):
        """解析HAVING子句"""
        self.consume('HAVING', '期望 HAVING')
        
        # 解析条件表达式（与WHERE子句相同）
        condition = self.parse_condition()
        return condition

    
    def parse_update(self):
        """解析UPDATE语句"""
        self.consume('UPDATE', '期望 UPDATE')
        table_name = self.get_token_value(self.consume_id('期望表名'))
        
        self.consume('SET', '期望 SET')
        assignments = self.parse_assignment_list()
        
        where_clause = None
        if self.match('WHERE'):
            where_clause = self.parse_where_clause()
        
        return Update(table_name, assignments, where_clause)
    
    def parse_assignment_list(self):
        """解析赋值列表"""
        assignments = []
        assignments.append(self.parse_assignment())
        
        while self.match(','):
            self.advance()  # 消费逗号
            assignments.append(self.parse_assignment())
        
        return assignments
    
    def parse_assignment(self):
        """解析赋值"""
        column = self.get_token_value(self.consume_id('期望列名'))
        self.consume('=', '期望 =')
        value = self.parse_expression()  # 修改：使用 parse_expression 而不是 parse_value
        
        return Assignment(column, value)
    
    def parse_delete(self):
        """解析DELETE语句"""
        self.consume('DELETE', '期望 DELETE')
        self.consume('FROM', '期望 FROM')
        
        table_name = self.get_token_value(self.consume_id('期望表名'))
        
        where_clause = None
        if self.match('WHERE'):
            where_clause = self.parse_where_clause()
        
        return Delete(table_name, where_clause)
    

    def parse_drop_statement(self):
        """解析DROP语句"""
        self.consume('DROP', '期望 DROP')
        
        if self.match('TABLE'):
            return self.parse_drop_table()
        elif self.match('VIEW'):
            return self.parse_drop_view()
        elif self.match('INDEX'):
            return self.parse_drop_index()
        elif self.match('TRIGGER'):
            return self.parse_drop_trigger()
        else:
            self.error(f"期望 TABLE, VIEW, INDEX 或 TRIGGER，得到 {self.peek()}")
    
    def parse_drop_table(self):
        """解析DROP TABLE语句"""
        self.consume('TABLE', '期望 TABLE')
        
        # 检查是否有 IF EXISTS
        if_exists = False
        if self.match('IF'):
            self.advance()  # 消费 IF
            self.consume('EXISTS', '期望 EXISTS')
            if_exists = True
        
        table_name = self.get_token_value(self.consume_id('期望表名'))
        return DropTable(table_name, if_exists)
    
    def parse_drop_view(self):
        """解析DROP VIEW语句"""
        self.consume('VIEW', '期望 VIEW')
        view_name = self.get_token_value(self.consume_id('期望视图名'))
        return DropView(view_name)
    
    def parse_drop_index(self):
        """解析DROP INDEX语句"""
        self.consume('INDEX', '期望 INDEX')
        index_name = self.get_token_value(self.consume_id('期望索引名'))
        return DropIndex(index_name)
    
    def parse_drop_trigger(self):
        """解析DROP TRIGGER语句"""
        self.consume('TRIGGER', '期望 TRIGGER')
        trigger_name_token = self.consume_id('期望触发器名')
        trigger_name = Identifier(trigger_name_token, self.get_token_value(trigger_name_token))
        return DropTriggerStatement(trigger_name)
    
    def parse_alter_statement(self):
        """解析ALTER语句"""
        self.consume('ALTER', '期望 ALTER')
        
        if self.match('TABLE'):
            return self.parse_alter_table()
        elif self.match('VIEW'):
            return self.parse_alter_view()
        else:
            self.error(f"期望 TABLE 或 VIEW，得到 {self.peek()}")
    
    def parse_alter_table(self):
        """解析ALTER TABLE语句"""
        self.consume('TABLE', '期望 TABLE')
        table_name = self.get_token_value(self.consume_id('期望表名'))
        
        if self.match('ADD'):
            self.advance()
            self.consume('COLUMN', '期望 COLUMN')
            column_def = self.parse_column_definition()
            return AlterTableAddColumn(table_name, column_def)
        else:
            self.error(f"期望 ADD，得到 {self.peek()}")
    
    def parse_alter_view(self):
        """解析ALTER VIEW语句"""
        self.consume('VIEW', '期望 VIEW')
        view_name = self.get_token_value(self.consume_id('期望视图名'))
        self.consume('AS', '期望 AS')
        
        # 解析新的SELECT语句
        select_statement = self.parse_select()
        return AlterView(view_name, select_statement)

    
    def parse_show(self):
        """解析SHOW语句"""
        self.consume('SHOW', '期望 SHOW')
        
        if self.match('TABLES'):
            self.advance()
            return ShowTables()
        elif self.match('COLUMNS'):
            self.advance()
            self.consume('FROM', '期望 FROM')
            table_name = self.get_token_value(self.consume_id('期望表名'))
            return ShowColumns(table_name)
        elif self.match('INDEX'):
            self.advance()
            self.consume('FROM', '期望 FROM')
            table_name = self.get_token_value(self.consume_id('期望表名'))
            return ShowIndex(table_name)
        elif self.match('TRIGGERS'):
            self.advance()
            return ShowTriggersStatement()
        elif self.match('VIEWS'):
            self.advance()
            return ShowViews()
        else:
            self.error(f"期望 TABLES, COLUMNS, INDEX, TRIGGERS 或 VIEWS，得到 {self.peek()}")
    
    def parse_explain(self):
        """解析EXPLAIN语句"""
        self.consume('EXPLAIN', '期望 EXPLAIN')
        
        # 解析要解释的查询
        query = self.parse_query()
        return Explain(query)
    
    # 辅助方法
    def match(self, *values):
        """检查当前token是否匹配指定值"""
        if self.is_at_end():
            return False
        
        token = self.peek()
        # 处理tuple格式的token: (type, value, line, column)
        if isinstance(token, tuple):
            token_type = token[0]
            token_value = token[1]
            
            # 首先检查是否为类型匹配（如 match('ID', 'NUMBER') 等）
            if any(value in ['ID', 'KEYWORD', 'NUM', 'STRING', 'OP', 'LPAREN', 'RPAREN', 'COMMA', 'SEMICOLON'] for value in values):
                return token_type in values
            
            # 对于关键字，进行大小写不敏感的比较
            # 同时支持KEYWORD和ID类型，以处理词法分析器可能将关键字识别为ID的情况
            if token_type == 'KEYWORD' or token_type == 'ID':
                return any(token_value.upper() == value.upper() for value in values)
            elif token_type == 'OP':
                # 对于操作符，比较值
                return token_value in values
            elif token_type == ';':
                # 对于分号，比较值
                return ';' in values
            elif token_type == ',':
                # 对于逗号，比较值
                return ',' in values
            elif token_type in ['(', ')']:
                # 对于括号，比较值
                return token_value in values
            else:
                # 对于其他类型，比较类型
                return token_type in values
        else:
            # 对于关键字，进行大小写不敏感的比较
            # 同时支持KEYWORD和ID类型，以处理词法分析器可能将关键字识别为ID的情况
            if token.type == 'KEYWORD' or token.type == 'ID':
                return any(token.value.upper() == value.upper() for value in values)
            else:
                return token.value in values
    
    def consume(self, value, message):
        """消费指定值的token"""
        if self.match(value):
            return self.advance()
        
        self.error(message)
    
    def consume_id(self, message):
        """消费ID类型的token"""
        if self.is_at_end():
            self.error(message)
        
        token = self.peek()
        if isinstance(token, tuple) and token[0] == 'ID':
            return self.advance()
        else:
            self.error(message)
    
    def consume_num(self, message):
        """消费NUM类型的token"""
        if self.is_at_end():
            self.error(message)
        
        token = self.peek()
        if isinstance(token, tuple) and token[0] == 'NUMBER':
            return self.advance()
        else:
            self.error(message)
    
    def consume_string(self, message):
        """消费STRING类型的token"""
        if self.is_at_end():
            self.error(message)
        
        token = self.peek()
        if isinstance(token, tuple) and token[0] == 'STRING':
            return self.advance()
        else:
            self.error(message)
    
    def advance(self):
        """前进到下一个token"""
        if not self.is_at_end():
            self.current += 1
        return self.previous()
    
    def peek(self):
        """查看当前token"""
        if self.is_at_end():
            return Token('EOF', 'EOF', 0, 0)
        return self.tokens[self.current]
    
    def previous(self):
        """获取前一个token"""
        return self.tokens[self.current - 1]
    
    def is_at_end(self):
        """检查是否到达token流末尾"""
        return self.current >= len(self.tokens)
    
    def is_join_keyword(self):
        """检查是否是JOIN关键字"""
        return (self.match('JOIN') or 
                self.match('INNER') or 
                self.match('LEFT') or 
                self.match('RIGHT') or 
                self.match('FULL'))
    
    def check(self, value):
        """检查当前token是否匹配指定值（不消费token）"""
        if self.is_at_end():
            return False
        
        token = self.peek()
        if isinstance(token, tuple):
            return token[0] == value or token[1] == value
        else:
            return getattr(token, 'type', '') == value or getattr(token, 'value', '') == value
    
    def error(self, message):
        """记录错误"""
        self.errors.append(message)
        raise SyntaxError(message)
    
    def get_token_value(self, token):
        """获取token的值"""
        if isinstance(token, tuple):
            return token[1]  # tuple格式: (type, value, line, column)
        else:
            return token.value
    
    # --- 游标相关解析方法 ---
    def parse_declare_cursor(self):
        """解析DECLARE CURSOR语句"""
        self.consume('DECLARE', '期望 DECLARE')
        cursor_name_token = self.consume_id('期望游标名')
        cursor_name = Identifier(cursor_name_token, self.get_token_value(cursor_name_token))
        self.consume('CURSOR', '期望 CURSOR')
        self.consume('FOR', '期望 FOR')
        query = self.parse_select()
        return DeclareCursorStatement(cursor_name=cursor_name, query=query)

    def parse_open_cursor(self):
        """解析OPEN CURSOR语句"""
        self.consume('OPEN', '期望 OPEN')
        cursor_name_token = self.consume_id('期望游标名')
        cursor_name = Identifier(cursor_name_token, self.get_token_value(cursor_name_token))
        return OpenCursorStatement(cursor_name=cursor_name)

    def parse_fetch_cursor(self):
        """解析FETCH CURSOR语句"""
        self.consume('FETCH', '期望 FETCH')
        cursor_name_token = self.consume_id('期望游标名')
        cursor_name = Identifier(cursor_name_token, self.get_token_value(cursor_name_token))
        return FetchCursorStatement(cursor_name=cursor_name)

    def parse_close_cursor(self):
        """解析CLOSE CURSOR语句"""
        self.consume('CLOSE', '期望 CLOSE')
        cursor_name_token = self.consume_id('期望游标名')
        cursor_name = Identifier(cursor_name_token, self.get_token_value(cursor_name_token))
        return CloseCursorStatement(cursor_name=cursor_name)


# AST节点定义
class Program:
    def __init__(self, query):
        self.query = query
    
    def __repr__(self):
        return f"Program({self.query})"


class CreateTable:
    def __init__(self, table_name, columns, constraints=None):
        self.table_name = table_name
        self.columns = columns
        self.constraints = constraints or []
    
    def __repr__(self):
        return f"CreateTable({self.table_name}, {self.columns}, {self.constraints})"


class ColumnDefinition:
    def __init__(self, name, data_type, constraints=None):
        self.name = name
        self.data_type = data_type
        self.constraints = constraints or []
    
    def __repr__(self):
        constraints_str = f", constraints={self.constraints}" if self.constraints else ""
        return f"ColumnDefinition({self.name}, {self.data_type}{constraints_str})"


class DataType:
    def __init__(self, type_name, arg1=None, arg2=None):
        self.type_name = type_name
        self.arg1 = arg1
        self.arg2 = arg2
    
    def __repr__(self):
        if self.arg1 is not None and self.arg2 is not None:
            return f"DataType({self.type_name}, {self.arg1}, {self.arg2})"
        elif self.arg1 is not None:
            return f"DataType({self.type_name}, {self.arg1})"
        else:
            return f"DataType({self.type_name})"


class Insert:
    def __init__(self, table_name, values, columns=None):
        self.table_name = table_name
        self.values = values
        self.columns = columns
    
    def __repr__(self):
        return f"Insert({self.table_name}, {self.values})"


class Value:
    def __init__(self, type_name, value):
        self.type_name = type_name
        self.value = value
    
    def __repr__(self):
        return f"Value({self.type_name}, {self.value})"


class Select:

    def __init__(self, columns, table_name, where_clause=None, order_by=None, joins=None, group_by=None, having_clause=None, limit=None):

        self.columns = columns
        self.table_name = table_name
        self.where_clause = where_clause
        self.order_by = order_by
        self.joins = joins or []
        self.group_by = group_by
        self.having_clause = having_clause
        self.limit = limit
    
    def __repr__(self):
        return f"Select({self.columns}, {self.table_name}, {self.where_clause}, {self.order_by}, {self.joins}, {self.group_by}, {self.having_clause})"



class Condition:
    def __init__(self, left, operator, right):
        self.left = left
        self.operator = operator
        self.right = right
    
    def __repr__(self):
        return f"Condition({self.left} {self.operator} {self.right})"


class BetweenCondition:
    def __init__(self, left, value1, value2):
        self.left = left
        self.value1 = value1
        self.value2 = value2
    
    def __repr__(self):
        return f"BetweenCondition({self.left} BETWEEN {self.value1} AND {self.value2})"


class OrCondition:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.op = 'OR'  # 添加op属性以兼容语义分析器
    
    def __repr__(self):
        return f"OrCondition({self.left} OR {self.right})"


class AndCondition:
    def __init__(self, left, right):
        self.left = left
        self.right = right
        self.op = 'AND'  # 添加op属性以兼容语义分析器
    
    def __repr__(self):
        return f"AndCondition({self.left} AND {self.right})"


class OrderBy:
    def __init__(self, column, direction):
        self.column = column
        self.direction = direction
    
    def __repr__(self):
        return f"OrderBy({self.column}, {self.direction})"


class OrderByList:
    def __init__(self, order_columns):
        self.order_columns = order_columns
    
    def __repr__(self):
        columns_str = ', '.join([f"{col.column} {col.direction}" for col in self.order_columns])
        return f"OrderByList({columns_str})"


class GroupBy:
    def __init__(self, columns):
        self.columns = columns
    
    def __repr__(self):
        return f"GroupBy({', '.join(self.columns)})"


class Limit:
    def __init__(self, limit, offset=0):
        self.limit = limit
        self.offset = offset
    
    def __repr__(self):
        if self.offset > 0:
            return f"Limit({self.limit}, offset={self.offset})"
        else:
            return f"Limit({self.limit})"


class ColumnWithAlias:
    def __init__(self, column_name, alias):
        self.column_name = column_name
        self.alias = alias
    
    def __repr__(self):
        return f"ColumnWithAlias({self.column_name} AS {self.alias})"


class Update:
    def __init__(self, table_name, assignments, where_clause=None):
        self.table_name = table_name
        self.assignments = assignments
        self.where_clause = where_clause
    
    def __repr__(self):
        return f"Update({self.table_name}, {self.assignments}, {self.where_clause})"


class Assignment:
    def __init__(self, column, value):
        self.column = column
        self.value = value
    
    def __repr__(self):
        return f"Assignment({self.column} = {self.value})"


class Delete:
    def __init__(self, table_name, where_clause=None):
        self.table_name = table_name
        self.where_clause = where_clause
    
    def __repr__(self):
        return f"Delete({self.table_name}, {self.where_clause})"


class DropTable:
    def __init__(self, table_name, if_exists=False):
        self.table_name = table_name
        self.if_exists = if_exists
    
    def __repr__(self):
        return f"DropTable({self.table_name}, if_exists={self.if_exists})"


class DropIndex:
    def __init__(self, index_name):
        self.index_name = index_name
    
    def __repr__(self):
        return f"DropIndex({self.index_name})"


class ShowTables:
    def __init__(self):
        pass
    
    def __repr__(self):
        return "ShowTables()"


class ShowViews:
    def __init__(self):
        pass
    
    def __repr__(self):
        return "ShowViews()"


class ShowColumns:
    def __init__(self, table_name):
        self.table_name = table_name
    
    def __repr__(self):
        return f"ShowColumns({self.table_name})"


class ShowIndex:
    def __init__(self, table_name):
        self.table_name = table_name
    
    def __repr__(self):
        return f"ShowIndex({self.table_name})"


class Explain:
    def __init__(self, query):
        self.query = query
    
    def __repr__(self):
        return f"Explain({self.query})"


class CreateIndex:
    def __init__(self, index_name, table_name, column_or_columns):
        self.index_name = index_name
        self.table_name = table_name
        # 支持单列或多列
        if isinstance(column_or_columns, list):
            self.columns = column_or_columns
            self.column_name = column_or_columns[0]  # 保持向后兼容性
        else:
            self.column_name = column_or_columns
            self.columns = [column_or_columns]
    
    def __repr__(self):
        if len(self.columns) == 1:
            return f"CreateIndex({self.index_name}, {self.table_name}, {self.column_name})"
        else:
            return f"CreateIndex({self.index_name}, {self.table_name}, {self.columns})"


class TableReference:
    def __init__(self, table_name, alias=None):
        self.table_name = table_name
        self.alias = alias
    
    def __repr__(self):
        if self.alias:
            return f"TableReference({self.table_name} AS {self.alias})"
        return f"TableReference({self.table_name})"


class MultiTableReference:
    def __init__(self, tables):
        self.tables = tables
    
    def __repr__(self):
        return f"MultiTableReference({self.tables})"


class Join:
    def __init__(self, join_type, right_table, condition):
        self.join_type = join_type
        self.right_table = right_table
        self.condition = condition
    
    def __repr__(self):
        return f"Join({self.join_type}, {self.right_table}, {self.condition})"


class JoinCondition:
    def __init__(self, left_table, left_column, operator, right_table, right_column):
        self.left_table = left_table
        self.left_column = left_column
        self.operator = operator
        self.right_table = right_table
        self.right_column = right_column
    
    def __repr__(self):
        return f"JoinCondition({self.left_table}.{self.left_column} {self.operator} {self.right_table}.{self.right_column})"



class InCondition:
    def __init__(self, left, values, negated=False):
        self.left = left
        self.values = values
        self.negated = negated
        # 为了兼容性，添加right和operator属性
        self.right = values
        self.operator = "IN"
    
    def __repr__(self):
        op = "NOT IN" if self.negated else "IN"
        values_str = "(" + ", ".join(str(v) for v in self.values) + ")"
        return f"InCondition({self.left} {op} {values_str})"


class InSubqueryCondition:
    def __init__(self, left, subquery, negated=False):
        self.left = left
        self.subquery = subquery
        self.negated = negated
    
    def __repr__(self):
        op = "NOT IN" if self.negated else "IN"
        return f"InSubqueryCondition({self.left} {op} ({self.subquery}))"


class ExistsCondition:
    def __init__(self, subquery, negated=False):
        self.subquery = subquery
        self.negated = negated
        # 为了兼容性，添加left、right和operator属性
        self.left = None
        self.right = subquery
        self.operator = "EXISTS"
    
    def __repr__(self):
        op = "NOT EXISTS" if self.negated else "EXISTS"
        return f"ExistsCondition({op} ({self.subquery}))"


# 视图相关的AST节点
class CreateView:
    def __init__(self, view_name, select_statement):
        self.view_name = view_name
        self.select_statement = select_statement
    
    def __repr__(self):
        return f"CreateView({self.view_name}, {self.select_statement})"


class DropView:
    def __init__(self, view_name):
        self.view_name = view_name
    
    def __repr__(self):
        return f"DropView({self.view_name})"


class AlterView:
    def __init__(self, view_name, select_statement):
        self.view_name = view_name
        self.select_statement = select_statement
    
    def __repr__(self):
        return f"AlterView({self.view_name}, {self.select_statement})"


class DropIndex:
    def __init__(self, index_name):
        self.index_name = index_name
    
    def __repr__(self):
        return f"DropIndex({self.index_name})"


class AlterTableAddColumn:
    def __init__(self, table_name, column_definition):
        self.table_name = table_name
        self.column_definition = column_definition
    
    def __repr__(self):
        return f"AlterTableAddColumn({self.table_name}, {self.column_definition})"


class SubqueryReference:
    def __init__(self, subquery, alias):
        self.subquery = subquery
        self.alias = alias
    
    def __repr__(self):
        return f"SubqueryReference({self.subquery}, AS {self.alias})"


class BeginTransactionStatement:
    """BEGIN TRANSACTION语句"""
    def __init__(self):
        self.type = 'BeginTransactionStatement'
    
    def __repr__(self):
        return "BeginTransactionStatement()"


class CommitTransactionStatement:
    """COMMIT TRANSACTION语句"""
    def __init__(self):
        self.type = 'CommitTransactionStatement'
    
    def __repr__(self):
        return "CommitTransactionStatement()"


class RollbackTransactionStatement:
    """ROLLBACK TRANSACTION语句"""
    def __init__(self):
        self.type = 'RollbackTransactionStatement'
    
    def __repr__(self):
        return "RollbackTransactionStatement()"


class Token:
    def __init__(self, type, value, line, column):
        self.type = type
        self.value = value
        self.line = line
        self.column = column
    
    def __repr__(self):
        return f"Token({self.type}, {self.value})"
