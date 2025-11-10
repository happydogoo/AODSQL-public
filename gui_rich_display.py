#!/usr/bin/env python3
"""
AODSQL GUI Rich 显示组件
使用 rich 库提供美化的输出格式
"""

import sys
import os
from typing import Any, List, Dict, Optional, Union
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.text import Text
from rich.syntax import Syntax
from rich.tree import Tree
from rich import box
from rich.align import Align
from rich.columns import Columns
from rich.rule import Rule
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.live import Live
from rich.layout import Layout
from rich.markdown import Markdown

class RichDisplayManager:
    """Rich 显示管理器"""
    
    def __init__(self):
        self.console = Console()
        
    def format_sql_result(self, result: Any, operation_type: str = "SELECT") -> str:
        """格式化SQL执行结果"""
        if not result:
            return self._format_empty_result()
        
        if isinstance(result, str):
            return self._format_string_result(result, operation_type)
        elif isinstance(result, list):
            return self._format_list_result(result, operation_type)
        else:
            return self._format_unknown_result(result)
    
    def _format_empty_result(self) -> str:
        """格式化空结果"""
        panel = Panel(
            Text("无数据", style="dim"),
            title="查询结果",
            border_style="blue",
            box=box.ROUNDED
        )
        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()
    
    def _format_string_result(self, result: str, operation_type: str) -> str:
        """格式化字符串结果"""
        if operation_type == "SHOW_TABLES":
            return self._format_show_tables(result)
        elif operation_type in ["CREATE_TABLE", "DROP_TABLE", "INSERT", "UPDATE", "DELETE"]:
            return self._format_ddl_result(result, operation_type)
        else:
            return self._format_general_string(result)
    
    def _format_show_tables(self, result: str) -> str:
        """格式化 SHOW TABLES 结果"""
        if "无数据" in result or "No tables found" in result:
            panel = Panel(
                Text("暂无表", style="dim"),
                title="📋 表列表",
                border_style="blue",
                box=box.ROUNDED
            )
            with self.console.capture() as capture:
                self.console.print(panel)
            return capture.get()
        
        # 解析表名列表
        lines = result.strip().split('\n')
        table = Table(title="📋 数据库表", box=box.ROUNDED, border_style="blue")
        table.add_column("序号", style="cyan", width=6)
        table.add_column("表名", style="green", width=20)
        table.add_column("状态", style="yellow", width=10)
        
        for i, line in enumerate(lines, 1):
            if line.strip():
                table_name = line.strip()
                table.add_row(str(i), table_name, "✅ 正常")
        
        with self.console.capture() as capture:
            self.console.print(table)
        return capture.get()
    
    def _format_ddl_result(self, result: str, operation_type: str) -> str:
        """格式化DDL操作结果"""
        icons = {
            "CREATE_TABLE": "🏗️",
            "DROP_TABLE": "🗑️", 
            "INSERT": "➕",
            "UPDATE": "✏️",
            "DELETE": "🗑️"
        }
        
        colors = {
            "CREATE_TABLE": "green",
            "DROP_TABLE": "red",
            "INSERT": "blue", 
            "UPDATE": "yellow",
            "DELETE": "red"
        }
        
        icon = icons.get(operation_type, "📝")
        color = colors.get(operation_type, "white")
        
        panel = Panel(
            Text(result, style=color),
            title=f"{icon} {operation_type} 操作",
            border_style=color,
            box=box.ROUNDED
        )
        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()
    
    def _format_general_string(self, result: str) -> str:
        """格式化一般字符串结果"""
        panel = Panel(
            Text(result),
            title="执行结果",
            border_style="blue",
            box=box.ROUNDED
        )
        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()
    
    def _format_list_result(self, result: List, operation_type: str) -> str:
        """格式化列表结果"""
        if not result:
            return self._format_empty_result()
        
        # 检查是否是查询结果格式 [(row_id, (col1, col2, ...)), ...]
        if isinstance(result[0], tuple) and len(result[0]) == 2:
            return self._format_query_result(result)
        else:
            return self._format_simple_list(result)
    
    def _format_query_result(self, result: List[tuple]) -> str:
        """格式化查询结果"""
        if not result:
            return self._format_empty_result()
        
        # 获取列数
        first_row = result[0][1]
        if isinstance(first_row, tuple):
            num_cols = len(first_row)
        else:
            num_cols = 1
        
        # 创建表格
        table = Table(title="📊 查询结果", box=box.ROUNDED, border_style="green")
        
        # 添加列标题
        for i in range(num_cols):
            table.add_column(f"列 {i+1}", style="cyan", width=15)
        
        # 添加数据行
        for row_id, row_data in result:
            if isinstance(row_data, tuple):
                table.add_row(*[str(cell) for cell in row_data])
            else:
                table.add_row(str(row_data))
        
        # 添加统计信息
        footer = f"共 {len(result)} 行数据"
        table.caption = footer
        
        with self.console.capture() as capture:
            self.console.print(table)
        return capture.get()
    
    def _format_simple_list(self, result: List) -> str:
        """格式化简单列表结果"""
        table = Table(title="📋 列表结果", box=box.ROUNDED, border_style="blue")
        table.add_column("序号", style="cyan", width=6)
        table.add_column("内容", style="white", width=30)
        
        for i, item in enumerate(result, 1):
            table.add_row(str(i), str(item))
        
        with self.console.capture() as capture:
            self.console.print(table)
        return capture.get()
    
    def _format_unknown_result(self, result: Any) -> str:
        """格式化未知类型结果"""
        panel = Panel(
            Text(str(result), style="dim"),
            title="执行结果",
            border_style="yellow",
            box=box.ROUNDED
        )
        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()
    
    def format_sql_syntax(self, sql: str) -> str:
        """格式化SQL语法高亮"""
        syntax = Syntax(
            sql,
            "sql",
            theme="monokai",
            line_numbers=True,
            word_wrap=True
        )
        with self.console.capture() as capture:
            self.console.print(syntax)
        return capture.get()
    
    def format_system_info(self, info: Dict[str, Any]) -> str:
        """格式化系统信息"""
        layout = Layout()
        
        # 创建信息面板
        info_text = Text()
        info_text.append("AODSQL 数据库系统\n", style="bold blue")
        info_text.append(f"版本: {info.get('version', '1.0.0')}\n")
        info_text.append(f"当前数据库: {info.get('current_db', '未选择')}\n")
        info_text.append(f"表数量: {info.get('table_count', 0)}\n")
        info_text.append(f"状态: {info.get('status', '运行中')}")
        
        panel = Panel(
            info_text,
            title="系统信息",
            border_style="green",
            box=box.ROUNDED
        )
        
        with self.console.capture() as capture:
            self.console.print(panel)
        return capture.get()
    
    def format_error(self, error: str) -> str:
        """格式化错误信息 - 简化版"""
        return f"❌ {error}"
    
    def format_success(self, message: str) -> str:
        """格式化成功信息 - 简化版"""
        return f"✅ {message}"
    
    def format_warning(self, message: str) -> str:
        """格式化警告信息 - 简化版"""
        return f"⚠️ {message}"
    
    def format_table_schema(self, table_name: str, columns: List[Dict]) -> str:
        """格式化表结构信息 - 改进版"""
        result = []
        result.append("=" * 60)
        result.append(f"📋 表结构: {table_name}")
        result.append("=" * 60)
        
        # 创建表头
        header = "│ 列名              │ 数据类型        │ 主键  │ 非空  │"
        separator = "├──────────────────┼─────────────────┼───────┼───────┤"
        bottom = "└──────────────────┴─────────────────┴───────┴───────┘"
        
        result.append(header)
        result.append(separator)
        
        for col in columns:
            name = col.get('name', '')
            data_type = col.get('type', '')
            primary_key = "是" if col.get('primary_key', False) else "否"
            not_null = "是" if col.get('not_null', False) else "否"
            
            # 格式化列名（限制长度）
            name_display = name[:16] + ".." if len(name) > 16 else name
            data_type_display = data_type[:15] + ".." if len(data_type) > 15 else data_type
            
            row = f"│ {name_display:<16} │ {data_type_display:<15} │ {primary_key:<5} │ {not_null:<5} │"
            result.append(row)
        
        result.append(bottom)
        result.append(f"共 {len(columns)} 列")
        result.append("=" * 60)
        
        return "\n".join(result)
    
    def format_execution_plan(self, plan: Dict[str, Any]) -> str:
        """格式化执行计划 - 简化版"""
        plan_type = plan.get('type', 'PhysicalPlan')
        
        if plan_type == 'PhysicalPlanWithAnalysis':
            title = "🌲 执行计划 (EXPLAIN ANALYZE)"
            analysis = plan.get('properties', {}).get('analysis', '')
            if analysis:
                title += f"\n📊 分析: {analysis}"
        else:
            title = "🌲 执行计划 (EXPLAIN)"
        
        result = [title]
        result.append("=" * 50)
        
        def format_node(node, level=0):
            if isinstance(node, dict):
                node_type = node.get('type', 'Unknown')
                properties = node.get('properties', {})
                
                indent = "  " * level
                node_text = f"{indent}└─ {node_type}"
                
                # 只显示重要属性
                important_props = {}
                for k, v in properties.items():
                    if k not in ['sql', 'analysis'] and v is not None:
                        if isinstance(v, (list, tuple)) and len(v) > 3:
                            important_props[k] = f"[{len(v)} items]"
                        else:
                            important_props[k] = str(v)
                
                if important_props:
                    props_text = ", ".join([f"{k}={v}" for k, v in important_props.items()])
                    node_text += f" ({props_text})"
                
                result.append(node_text)
                
                # 添加子节点
                children = node.get('children', [])
                for child in children:
                    format_node(child, level + 1)
            else:
                indent = "  " * level
                result.append(f"{indent}└─ {str(node)}")
        
        # 添加根节点
        children = plan.get('children', [])
        for child in children:
            format_node(child)
        
        return "\n".join(result)
    
    def format_select_result(self, headers: List[str], rows: List[List]) -> str:
        """格式化SELECT查询结果 - 改进版"""
        if not headers and not rows:
            return "📊 查询结果: 无数据"
        
        # 创建更醒目的表格
        result = []
        result.append("=" * 60)
        result.append("📊 查询结果")
        result.append("=" * 60)
        
        # 计算列宽
        col_widths = []
        for i, header in enumerate(headers):
            max_width = len(header)
            for row in rows:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            col_widths.append(max_width + 2)  # 加2个空格作为边距
        
        # 创建表头
        header_line = "│"
        separator_line = "├"
        for i, (header, width) in enumerate(zip(headers, col_widths)):
            header_line += f" {header:<{width-1}}│"
            separator_line += "─" * width + "┼"
        separator_line = separator_line[:-1] + "┤"  # 替换最后一个┼为┤
        
        result.append(header_line)
        result.append(separator_line)
        
        # 添加数据行
        for row in rows:
            row_line = "│"
            for i, (value, width) in enumerate(zip(row, col_widths)):
                if i < len(row):
                    row_line += f" {str(value):<{width-1}}│"
                else:
                    row_line += f" {'':<{width-1}}│"
            result.append(row_line)
        
        # 添加底部边框
        bottom_line = "└"
        for width in col_widths:
            bottom_line += "─" * width + "┴"
        bottom_line = bottom_line[:-1] + "┘"
        result.append(bottom_line)
        
        result.append(f"共 {len(rows)} 行数据")
        result.append("=" * 60)
        
        return "\n".join(result)
    
    def format_dml_result(self, message: str) -> str:
        """格式化DML操作结果 - 简化版"""
        return f"✅ {message}"
    
    def format_ddl_result(self, message: str) -> str:
        """格式化DDL操作结果 - 简化版"""
        return f"✅ {message}"
    
    def format_general_string(self, message: str) -> str:
        """格式化一般字符串消息 - 简化版"""
        return f"ℹ️ {message}"

# 全局实例
rich_display = RichDisplayManager()
