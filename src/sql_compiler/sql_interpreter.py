# -*- coding: utf-8 -*-
"""
SQL解释器 - 以算子树形式输出执行计划
"""
from typing import Dict, Any, List
from .logical_operators import LogicalPlan, LogicalOperator
from src.engine.catalog_manager import CatalogManager
from .symbol_table import SymbolTable
import sys
import os

# 添加路径以便导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


class SQLInterpreter:
    """SQL解释器 - 生成算子树形式的执行计划"""
    
    def __init__(self, symbol_table: SymbolTable, catalog_manager: CatalogManager):
        self.symbol_table = symbol_table
        self.catalog_manager = catalog_manager
        self.catalog_manager = catalog_manager
    
    def _extract_final_value_from_ast_node(self, node: Any) -> Any:
        """
        【强大的辅助函数】递归地、健壮地从任何AST节点或其字符串表示中提取最终的、干净的值。
        """
        for _ in range(10):
            if hasattr(node, 'value'):
                node = node.value
            else:
                break
        if isinstance(node, str):
            import re
            match = re.search(r"value=['\"]([^'\"]+)['\"]", node)
            if match:
                clean_value = match.group(1)
            else:
                clean_value = node
        else:
            clean_value = node
        if isinstance(clean_value, str):
            try:
                if '.' in clean_value:
                    return float(clean_value)
                else:
                    return int(clean_value)
            except (ValueError, TypeError):
                return clean_value
        return clean_value

    def interpret(self, sql_text: str) -> Dict[str, Any]:
        """
        【最终修复版】解释SQL语句，并使用强大的辅助函数来强制应用索引扫描。
        """
        try:
            from .lexicalAnalysis import tokenize
            tokens = tokenize(sql_text)
            from src.engine.view.query_rewriter import QueryRewriter
            query_rewriter = QueryRewriter(self.catalog_manager)
            rewritten_sql = query_rewriter.rewrite_query(sql_text) if query_rewriter.is_view_query(sql_text) else sql_text
            from .syntax_adapter import SyntaxAdapter
            syntax_analyzer = SyntaxAdapter(use_new_analyzer=True)
            ast = syntax_analyzer.build_ast_from_tokens(tokenize(rewritten_sql))
            from .enhanced_semantic_analyzer import EnhancedSemanticAnalyzer
            semantic_analyzer = EnhancedSemanticAnalyzer(self.symbol_table)
            if not semantic_analyzer.analyze(ast):
                errors = semantic_analyzer.get_errors()
                return {"status": "error", "message": f"语义分析失败: {errors[0]}" if errors else "未知语义错误"}
            from .enhanced_query_planner import EnhancedQueryPlanner
            planner = EnhancedQueryPlanner(self.symbol_table)
            logical_plan = planner.create_plan(ast)
            # --- 【最终版强制索引扫描逻辑】 ---
            try:
                from .logical_operators import FilterOperator, ScanOperator, IndexScanOperator, ProjectOperator
                root_op = logical_plan.root
                project_op = None
                filter_op = None
                scan_op = None
                if hasattr(root_op, 'operator_type') and getattr(root_op, 'operator_type', None) and getattr(root_op.operator_type, 'value', None) == 'Project' and root_op.children:
                    project_op = root_op
                    if isinstance(root_op.children[0], FilterOperator) and root_op.children[0].children:
                        filter_op = root_op.children[0]
                        if isinstance(filter_op.children[0], ScanOperator):
                            scan_op = filter_op.children[0]
                elif isinstance(root_op, FilterOperator) and root_op.children:
                    filter_op = root_op
                    if isinstance(filter_op.children[0], ScanOperator):
                        scan_op = filter_op.children[0]
                if filter_op and scan_op:
                    table_name = scan_op.table_name
                    condition = filter_op.condition
                    if hasattr(condition, 'operator') and hasattr(condition, 'left') and hasattr(condition, 'right'):
                        op_token = getattr(condition, 'operator', None)
                        op = op_token.literal if hasattr(op_token, 'literal') else ''
                        if op in ('=', '=='):
                            column_name = self._extract_final_value_from_ast_node(condition.left)
                            value = self._extract_final_value_from_ast_node(condition.right)
                            if isinstance(column_name, str) and self.catalog_manager.has_index_on(table_name, column_name):
                                print(f"\n[调试信息] 检测到可用索引，强制替换为 IndexScan...")
                                index_name = self.catalog_manager.get_index_by_column(table_name, column_name)
                                index_scan_op = IndexScanOperator(
                                    table_name=table_name,
                                    index_name=index_name,
                                    column_name=column_name,
                                    predicate={'key': (value,)}
                                )
                                if project_op:
                                    project_op.children = [index_scan_op]
                                else:
                                    logical_plan.root = index_scan_op
            except Exception as e:
                import traceback
                print(f"\n[调试信息] 强制索引替换失败: {e}\n{traceback.format_exc()}")
            # --- 强制逻辑结束 ---
            optimized_plan = logical_plan
            optimization_info = { "optimization_applied": False, "reason": "已使用强制索引规则，跳过优化器" }
            operator_tree = self._convert_to_operator_tree(optimized_plan)
            return {
                "status": "success", "sql": sql_text, "operator_tree": operator_tree,
                "metadata": { "optimization": optimization_info }
            }
        except Exception as e:
            import traceback
            return {
                "status": "error",
                "message": f"解释失败: {str(e)}\n{traceback.format_exc()}",
                "sql": sql_text
            }
    
    def _convert_to_operator_tree(self, logical_plan: LogicalPlan) -> Dict[str, Any]:
        """将逻辑计划转换为算子树格式"""
        return self._convert_operator_to_tree(logical_plan.root)
    
    def _convert_operator_to_tree(self, operator: LogicalOperator) -> Dict[str, Any]:
        """递归转换操作符为树格式"""
        # 获取操作符基本信息
        if hasattr(operator, 'operator_type'):
            if hasattr(operator, 'to_dict') and 'type' in operator.to_dict():
                # 对于特殊操作符（如CREATE_TABLE），使用to_dict中的type
                operator_type = operator.to_dict()['type']
            else:
                operator_type = operator.operator_type.value
        else:
            operator_type = type(operator).__name__
        
        operator_info = {
            "type": operator_type,
            "class": type(operator).__name__,
            "properties": self._extract_operator_properties(operator),
            "children": []
        }
        
        # 递归处理子操作符
        children = operator.get_children() if hasattr(operator, 'get_children') else []
        for child in children:
            child_tree = self._convert_operator_to_tree(child)
            operator_info["children"].append(child_tree)
        
        return operator_info
    
    def _extract_operator_properties(self, operator: LogicalOperator) -> Dict[str, Any]:
        """提取操作符的属性信息"""
        properties = {}
        
        # 根据操作符类型提取特定属性
        if hasattr(operator, 'table_name'):
            properties["table_name"] = operator.table_name
        
        if hasattr(operator, 'if_exists'):
            properties["if_exists"] = operator.if_exists
        
        if hasattr(operator, 'alias'):
            properties["alias"] = operator.alias
        
        if hasattr(operator, 'columns'):
            # 确保columns是可迭代的
            try:
                if hasattr(operator.columns, '__iter__'):
                    # 对于CREATE_TABLE操作符，需要特殊处理列信息
                    if hasattr(operator, 'to_dict') and 'type' in operator.to_dict() and operator.to_dict()['type'] == 'CREATE_TABLE':
                        # 使用to_dict方法获取列信息
                        operator_dict = operator.to_dict()
                        properties["columns"] = operator_dict.get('columns', [])
                    else:
                        properties["columns"] = [str(col) for col in operator.columns]
                else:
                    properties["columns"] = [str(operator.columns)]
            except TypeError:
                properties["columns"] = [str(operator.columns)]
        
        if hasattr(operator, 'condition'):
            properties["condition"] = str(operator.condition)
        
        # 触发器相关属性
        if hasattr(operator, 'trigger_name'):
            properties["trigger_name"] = operator.trigger_name
        if hasattr(operator, 'timing'):
            properties["timing"] = operator.timing
        if hasattr(operator, 'events'):
            properties["events"] = operator.events
        if hasattr(operator, 'is_row_level'):
            properties["is_row_level"] = operator.is_row_level
        if hasattr(operator, 'when_condition'):
            properties["when_condition"] = operator.when_condition
        if hasattr(operator, 'trigger_body'):
            properties["trigger_body"] = operator.trigger_body
        
        # 游标相关属性
        if hasattr(operator, 'cursor_name'):
            properties["cursor_name"] = operator.cursor_name
        if hasattr(operator, 'query_plan'):
            properties["query_plan"] = operator.query_plan.to_dict() if hasattr(operator.query_plan, 'to_dict') else str(operator.query_plan)
        
        if hasattr(operator, 'order_items'):
            properties["items"] = operator.order_items
        
        if hasattr(operator, 'where_clause'):
            properties["where_clause"] = str(operator.where_clause)
        
        if hasattr(operator, 'values'):
            # 处理多行VALUES
            if isinstance(operator.values, list) and len(operator.values) > 0 and isinstance(operator.values[0], list):
                # 多行VALUES格式：[[row1], [row2], ...]
                properties["values"] = [[str(val) for val in row] for row in operator.values]
            else:
                # 单行VALUES格式：[val1, val2, ...]
                properties["values"] = [str(val) for val in operator.values]
        
        if hasattr(operator, 'join_type'):
            properties["join_type"] = operator.join_type.value if hasattr(operator.join_type, 'value') else str(operator.join_type)
        
        if hasattr(operator, 'order_by'):
            properties["order_by"] = [str(col) for col in operator.order_by]
        
        # 处理IndexScanOperator的特有属性
        if hasattr(operator, 'index_name'):
            properties["index_name"] = operator.index_name
        
        if hasattr(operator, 'column_name'):
            properties["column_name"] = operator.column_name
        
        if hasattr(operator, 'predicate'):
            properties["predicate"] = operator.predicate
        
        if hasattr(operator, 'ascending'):
            properties["ascending"] = operator.ascending
        
        if hasattr(operator, 'limit'):
            properties["limit"] = operator.limit
        
        if hasattr(operator, 'offset'):
            properties["offset"] = operator.offset
        
        if hasattr(operator, 'set_clause'):
            properties["set_clause"] = operator.set_clause
 
        # 视图相关属性
        if hasattr(operator, 'view_name'):
            properties["view_name"] = operator.view_name
        
        if hasattr(operator, 'definition'):
            properties["definition"] = operator.definition
        
        if hasattr(operator, 'schema_name'):
            properties["schema_name"] = operator.schema_name
        
        if hasattr(operator, 'creator'):
            properties["creator"] = operator.creator
        
        if hasattr(operator, 'is_updatable'):
            properties["is_updatable"] = operator.is_updatable

        # 新增：提取优化器估算的成本和行数
        if hasattr(operator, 'estimated_cost'):
            properties["estimated_cost"] = operator.estimated_cost
        if hasattr(operator, 'estimated_rows'):
            properties["estimated_rows"] = operator.estimated_rows
        
        return properties

    
    def _get_symbol_table_info(self) -> Dict[str, Any]:
        """获取符号表信息"""
        tables_info = {}
        for table_name, table_info in self.symbol_table.tables.items():
            tables_info[table_name] = {
                "columns": [
                    {
                        "name": col.name,
                        "type": col.data_type.value,
                        "nullable": col.nullable,
                        "is_primary_key": col.is_primary_key
                    }
                    for col in table_info.columns
                ]
            }
        return tables_info
    
    def _is_select_statement(self, ast) -> bool:
        """判断是否为SELECT语句，避免误判DDL如CREATE TABLE等"""
        try:
            # 获取查询节点
            if hasattr(ast, 'query'):
                query_node = ast.query
            else:
                query_node = ast

            # DDL关键词集合
            ddl_keywords = ['create', 'drop', 'alter', 'index', 'database', 'table']

            # 检查AST的type、statement_type、name属性是否包含DDL关键词
            for attr in ['type', 'statement_type', 'name']:
                if hasattr(query_node, attr):
                    value = str(getattr(query_node, attr)).lower()
                    if any(kw in value for kw in ddl_keywords):
                        return False

            # 检查AST的类名
            class_name = query_node.__class__.__name__.lower()
            if any(kw in class_name for kw in ddl_keywords):
                return False

            # SELECT语句通常有columns和table属性
            if hasattr(query_node, 'columns') and hasattr(query_node, 'table'):
                return True
            if hasattr(query_node, 'columns') and hasattr(query_node, 'from_clause'):
                return True
            if hasattr(query_node, 'columns') and hasattr(query_node, 'tables'):
                return True

            return False
        except Exception:
            return False
    
    def print_operator_tree(self, sql_text: str):
        """打印算子树"""
        result = self.interpret(sql_text)
        
        if result["status"] == "error":
            print(f"❌ 解释失败: {result['message']}")
            return
        
        print("SQL解释器 - 算子树输出")
        print("=" * 60)
        print(f"SQL: {result['sql']}")
        print("-" * 60)
        
        operator_tree = result["operator_tree"]
        self._print_tree_recursive(operator_tree, 0)
        
        print("-" * 60)
        print("算子树结构:")
        self._print_tree_structure(operator_tree, 0)
    
    def _print_tree_recursive(self, node: Dict[str, Any], indent: int):
        """递归打印树结构"""
        prefix = "  " * indent
        op_type = node["type"]
        properties = node["properties"]
        
        # 打印操作符信息
        print(f"{prefix}├─ {op_type}")
        
        # 打印属性
        for key, value in properties.items():
            if isinstance(value, list) and len(value) > 3:
                print(f"{prefix}│  {key}: [{len(value)} items]")
            else:
                print(f"{prefix}│  {key}: {value}")
        
        # 递归打印子节点
        for child in node["children"]:
            self._print_tree_recursive(child, indent + 1)
    
    def _print_tree_structure(self, node: Dict[str, Any], indent: int):
        """打印树结构概览"""
        prefix = "  " * indent
        op_type = node["type"]
        children_count = len(node["children"])
        
        print(f"{prefix}{op_type}")
        
        for child in node["children"]:
            self._print_tree_structure(child, indent + 1)
    
    def get_operator_statistics(self, sql_text: str) -> Dict[str, Any]:
        """获取算子树统计信息"""
        result = self.interpret(sql_text)
        
        if result["status"] == "error":
            return {"error": result["message"]}
        
        operator_tree = result["operator_tree"]
        stats = self._calculate_statistics(operator_tree)
        
        return {
            "total_operators": stats["total"],
            "operator_types": stats["types"],
            "tree_depth": stats["depth"],
            "tree_width": stats["width"]
        }
    
    def _calculate_statistics(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """计算算子树统计信息"""
        stats = {
            "total": 1,
            "types": {node["type"]: 1},
            "depth": 1,
            "width": len(node["children"])
        }
        
        for child in node["children"]:
            child_stats = self._calculate_statistics(child)
            
            # 累加总数
            stats["total"] += child_stats["total"]
            
            # 合并类型统计
            for op_type, count in child_stats["types"].items():
                stats["types"][op_type] = stats["types"].get(op_type, 0) + count
            
            # 计算最大深度
            stats["depth"] = max(stats["depth"], child_stats["depth"] + 1)
            
            # 计算最大宽度
            stats["width"] = max(stats["width"], child_stats["width"])
        
        return stats


class OperatorTreeVisualizer:
    """算子树可视化器"""
    
    @staticmethod
    def visualize_tree(operator_tree: Dict[str, Any]) -> str:
        """生成算子树的文本可视化"""
        lines = []
        OperatorTreeVisualizer._build_tree_lines(operator_tree, lines, "", True, True)
        return "\n".join(lines)
    
    @staticmethod
    def _build_tree_lines(node: Dict[str, Any], lines: List[str], prefix: str, is_last: bool, is_root: bool):
        """构建树的可视化线条"""
        op_type = node["type"]
        properties = node["properties"]
        children = node["children"]
        
        # 当前节点
        if is_root:
            lines.append(f"┌─ {op_type}")
        else:
            connector = "└─" if is_last else "├─"
            lines.append(f"{prefix}{connector} {op_type}")
        
        # 属性信息
        if properties:
            prop_prefix = prefix + ("   " if is_last else "│  ")
            for i, (key, value) in enumerate(properties.items()):
                if isinstance(value, list) and len(value) > 3:
                    value_str = f"[{len(value)} items]"
                else:
                    value_str = str(value)
                
                is_last_prop = i == len(properties) - 1 and not children
                connector = "└─" if is_last_prop else "├─"
                lines.append(f"{prop_prefix}{connector} {key}: {value_str}")
        
        # 子节点
        if children:
            child_prefix = prefix + ("   " if is_last else "│  ")
            for i, child in enumerate(children):
                is_last_child = i == len(children) - 1
                OperatorTreeVisualizer._build_tree_lines(child, lines, child_prefix, is_last_child, False)

            def extract_numeric_value(val):
                """递归剥壳，最终提取数字类型（int/float），否则原样返回。"""
                if hasattr(val, 'value'):
                    return extract_numeric_value(val.value)
                if isinstance(val, str):
                    try:
                        if '.' in val:
                            return float(val)
                        else:
                            return int(val)
                    except Exception:
                        return val
                return val