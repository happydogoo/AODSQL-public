#!/usr/bin/env python3
"""
AODSQL GUI 主程序
使用 customtkinter 和 rich 库创建现代化的数据库管理界面
"""

import sys
import os
import threading
import queue
import tkinter as tk
from tkinter import ttk, scrolledtext
from typing import Dict, Any
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, 'cli'))

# 导入外部库
try:
    import customtkinter as ctk
    CUSTOM_TKINTER_AVAILABLE = True
except ImportError:
    CUSTOM_TKINTER_AVAILABLE = False
    print("警告: customtkinter 未安装，使用标准 tkinter")

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

# 导入AODSQL核心组件
from cli.system_manager import SystemManager

# 导入rich显示组件
from gui_rich_display import RichDisplayManager

class AODSQLGUI:
    """AODSQL 图形用户界面主类"""
    
    def __init__(self):
        # 选择GUI框架
        if CUSTOM_TKINTER_AVAILABLE:
            self.use_customtkinter = True
            self.root = ctk.CTk()
            ctk.set_appearance_mode("dark")
            ctk.set_default_color_theme("blue")
        else:
            self.use_customtkinter = False
            self.root = tk.Tk()
        
        self.root.title("AODSQL Database Management System")
        self.root.geometry("1400x900")
        self.root.minsize(1000, 700)
        
        # 初始化系统组件
        self.system_manager = None
        self.cli_interface = None
        self.rich_display = RichDisplayManager()
        
        # 创建GUI组件
        self.setup_gui()
        
        # 初始化数据库系统
        self.init_database_system()
        
    def setup_gui(self):
        """设置GUI界面布局"""
        if self.use_customtkinter:
            self.setup_customtkinter_gui()
        else:
            self.setup_standard_tkinter_gui()
    
    def setup_customtkinter_gui(self):
        """设置 CustomTkinter GUI"""
        # 主框架
        self.main_frame = ctk.CTkFrame(self.root)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 标题
        self.title_label = ctk.CTkLabel(
            self.main_frame, 
            text="AODSQL Database Management System - Enhanced",
            font=ctk.CTkFont(size=24, weight="bold")
        )
        self.title_label.pack(pady=(10, 20))
        
        # 创建左右分栏
        self.content_frame = ctk.CTkFrame(self.main_frame)
        self.content_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 左侧面板 - 数据库信息和控制
        self.left_panel = ctk.CTkFrame(self.content_frame)
        self.left_panel.pack(side="left", fill="y", padx=(0, 5))
        self.left_panel.configure(width=350)
        
        # 右侧面板 - SQL输入和结果
        self.right_panel = ctk.CTkFrame(self.content_frame)
        self.right_panel.pack(side="right", fill="both", expand=True, padx=(5, 0))
        
        self.setup_left_panel_customtkinter()
        self.setup_right_panel_customtkinter()
    
    def setup_standard_tkinter_gui(self):
        """设置标准 Tkinter GUI"""
        # 主框架
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 标题
        self.title_label = ttk.Label(
            self.main_frame, 
            text="AODSQL Database Management System - Enhanced",
            font=("Arial", 16, "bold")
        )
        self.title_label.pack(pady=(10, 20))
        
        # 创建左右分栏
        self.content_frame = ttk.Frame(self.main_frame)
        self.content_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        # 左侧面板 - 数据库信息和控制
        self.left_panel = ttk.LabelFrame(self.content_frame, text="数据库控制", padding=10)
        self.left_panel.pack(side="left", fill="y", padx=(0, 5))
        self.left_panel.configure(width=350)
        
        # 右侧面板 - SQL输入和结果
        self.right_panel = ttk.LabelFrame(self.content_frame, text="SQL 查询", padding=10)
        self.right_panel.pack(side="right", fill="both", expand=True, padx=(5, 0))
        
        self.setup_left_panel_standard()
        self.setup_right_panel_standard()
    
    def setup_left_panel_customtkinter(self):
        """设置 CustomTkinter 左侧控制面板 - 改进版"""
        # 1. 当前数据库状态卡片
        self.current_db_frame = ctk.CTkFrame(self.left_panel)
        self.current_db_frame.pack(fill="x", padx=10, pady=(10, 5))
        
        ctk.CTkLabel(self.current_db_frame, text="📊 当前状态", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(10, 5))
        
        self.current_db_label = ctk.CTkLabel(
            self.current_db_frame, 
            text="数据库: 未连接", 
            font=ctk.CTkFont(size=14)
        )
        self.current_db_label.pack(pady=2)
        
        self.table_count_label = ctk.CTkLabel(
            self.current_db_frame, 
            text="表数量: 0", 
            font=ctk.CTkFont(size=12)
        )
        self.table_count_label.pack(pady=2)
        
        # 2. 数据库管理区域
        self.db_management_frame = ctk.CTkFrame(self.left_panel)
        self.db_management_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(self.db_management_frame, text="🗄️ 数据库管理", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(10, 5))
        
        # 数据库选择下拉框
        self.db_selector_frame = ctk.CTkFrame(self.db_management_frame)
        self.db_selector_frame.pack(fill="x", padx=5, pady=5)
        
        ctk.CTkLabel(self.db_selector_frame, text="选择数据库:", font=ctk.CTkFont(size=12)).pack(side="left", padx=5)
        
        self.db_dropdown = ctk.CTkComboBox(
            self.db_selector_frame,
            values=["加载中..."],
            command=self.on_database_selected,
            font=ctk.CTkFont(size=12),
            width=150
        )
        self.db_dropdown.pack(side="right", padx=5, pady=5)
        
        # 数据库操作按钮
        self.db_buttons_frame = ctk.CTkFrame(self.db_management_frame)
        self.db_buttons_frame.pack(fill="x", padx=5, pady=5)
        
        self.refresh_db_btn = ctk.CTkButton(
            self.db_buttons_frame, 
            text="🔄 刷新",
            command=self.refresh_databases,
            font=ctk.CTkFont(size=12),
            width=80
        )
        self.refresh_db_btn.pack(side="left", padx=2)
        
        self.create_db_btn = ctk.CTkButton(
            self.db_buttons_frame,
            text="➕ 新建",
            command=self.create_database,
            font=ctk.CTkFont(size=12),
            width=80
        )
        self.create_db_btn.pack(side="left", padx=2)
        
        # 3. 表信息区域
        self.table_info_frame = ctk.CTkFrame(self.left_panel)
        self.table_info_frame.pack(fill="both", expand=True, padx=10, pady=5)
        
        ctk.CTkLabel(self.table_info_frame, text="📋 表信息", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(10, 5))
        
        # 表列表 - 使用更清晰的显示方式
        self.table_list_frame = ctk.CTkScrollableFrame(self.table_info_frame, height=200)
        self.table_list_frame.pack(fill="both", expand=True, padx=5, pady=5)
        
        # 表操作按钮
        self.table_buttons_frame = ctk.CTkFrame(self.table_info_frame)
        self.table_buttons_frame.pack(fill="x", padx=5, pady=5)
        
        self.refresh_tables_btn = ctk.CTkButton(
            self.table_buttons_frame,
            text="🔄 刷新表列表",
            command=self.refresh_tables,
            font=ctk.CTkFont(size=12)
        )
        self.refresh_tables_btn.pack(side="left", padx=2)
        
        
        # 4. 系统信息区域
        self.system_info_frame = ctk.CTkFrame(self.left_panel)
        self.system_info_frame.pack(fill="x", padx=10, pady=5)
        
        ctk.CTkLabel(self.system_info_frame, text="⚙️ 系统信息", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(10, 5))
        
        self.system_info_text = ctk.CTkTextbox(
            self.system_info_frame, 
            height=80, 
            font=ctk.CTkFont(size=12)
        )
        self.system_info_text.pack(fill="x", padx=5, pady=5)
        
        # 5. 高级功能区域
        self.advanced_buttons_frame = ctk.CTkFrame(self.left_panel)
        self.advanced_buttons_frame.pack(fill="x", padx=10, pady=(5, 10))
        
        ctk.CTkLabel(self.advanced_buttons_frame, text="🔧 高级功能", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(10, 5))
        
        # 高级功能按钮 - 使用网格布局
        self.advanced_buttons_grid = ctk.CTkFrame(self.advanced_buttons_frame)
        self.advanced_buttons_grid.pack(fill="x", padx=5, pady=5)
        
        self.show_triggers_btn = ctk.CTkButton(
            self.advanced_buttons_grid,
            text="🔧 触发器",
            command=self.show_triggers,
            font=ctk.CTkFont(size=11),
            width=80,
            height=30
        )
        self.show_triggers_btn.grid(row=0, column=0, padx=2, pady=2)
        
        self.show_views_btn = ctk.CTkButton(
            self.advanced_buttons_grid,
            text="👁️ 视图",
            command=self.show_views,
            font=ctk.CTkFont(size=11),
            width=80,
            height=30
        )
        self.show_views_btn.grid(row=0, column=1, padx=2, pady=2)
        
        self.show_indexes_btn = ctk.CTkButton(
            self.advanced_buttons_grid,
            text="📊 索引",
            command=self.show_indexes,
            font=ctk.CTkFont(size=11),
            width=80,
            height=30
        )
        self.show_indexes_btn.grid(row=1, column=0, padx=2, pady=2)
        
        self.performance_btn = ctk.CTkButton(
            self.advanced_buttons_grid,
            text="⚡ 性能",
            command=self.show_performance,
            font=ctk.CTkFont(size=11),
            width=80,
            height=30
        )
        self.performance_btn.grid(row=1, column=1, padx=2, pady=2)
    
    def setup_left_panel_standard(self):
        """设置标准 Tkinter 左侧控制面板"""
        # 数据库信息
        self.db_info_frame = ttk.LabelFrame(self.left_panel, text="数据库信息", padding=10)
        self.db_info_frame.pack(fill="x", pady=5)
        
        # 当前数据库
        self.current_db_label = ttk.Label(self.db_info_frame, text="当前数据库: 未连接")
        self.current_db_label.pack(pady=5)
        
        # 数据库列表
        self.db_list_label = ttk.Label(self.db_info_frame, text="可用数据库:")
        self.db_list_label.pack(pady=(10, 5))
        
        self.db_listbox = ttk.Frame(self.db_info_frame)
        self.db_listbox.pack(fill="x", pady=5)
        
        # 数据库操作按钮
        self.db_buttons_frame = ttk.Frame(self.left_panel)
        self.db_buttons_frame.pack(fill="x", pady=10)
        
        self.refresh_db_btn = ttk.Button(
            self.db_buttons_frame, 
            text="🔄 刷新数据库列表",
            command=self.refresh_databases
        )
        self.refresh_db_btn.pack(fill="x", pady=5)
        
        self.create_db_btn = ttk.Button(
            self.db_buttons_frame,
            text="➕ 创建新数据库",
            command=self.create_database
        )
        self.create_db_btn.pack(fill="x", pady=5)
        
        # 表信息
        self.table_info_frame = ttk.LabelFrame(self.left_panel, text="表信息", padding=10)
        self.table_info_frame.pack(fill="both", expand=True, pady=5)
        
        self.table_listbox = ttk.Frame(self.table_info_frame)
        self.table_listbox.pack(fill="both", expand=True, pady=5)
        
        self.refresh_tables_btn = ttk.Button(
            self.table_info_frame,
            text="🔄 刷新表列表",
            command=self.refresh_tables
        )
        self.refresh_tables_btn.pack(fill="x", pady=5)
    
    def setup_right_panel_customtkinter(self):
        """设置 CustomTkinter 右侧SQL输入和结果面板"""
        # SQL输入区域
        self.sql_input_frame = ctk.CTkFrame(self.right_panel)
        self.sql_input_frame.pack(fill="x", padx=10, pady=10)
        
        ctk.CTkLabel(self.sql_input_frame, text="SQL 查询", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(10, 5))
        
        # SQL输入文本框
        self.sql_textbox = ctk.CTkTextbox(
            self.sql_input_frame, 
            height=150,
            font=ctk.CTkFont(family="Consolas", size=16)
        )
        self.sql_textbox.pack(fill="x", padx=10, pady=5)
        
        # SQL操作按钮
        self.sql_buttons_frame = ctk.CTkFrame(self.sql_input_frame)
        self.sql_buttons_frame.pack(fill="x", padx=10, pady=10)
        
        self.execute_btn = ctk.CTkButton(
            self.sql_buttons_frame,
            text="▶️ 执行 SQL",
            command=self.execute_sql,
            font=ctk.CTkFont(size=16, weight="bold")
        )
        self.execute_btn.pack(side="left", padx=(0, 10))
        
        self.clear_btn = ctk.CTkButton(
            self.sql_buttons_frame,
            text="🗑️ 清空",
            command=self.clear_sql,
            font=ctk.CTkFont(size=14)
        )
        self.clear_btn.pack(side="left", padx=(0, 10))
        
        
        self.explain_btn = ctk.CTkButton(
            self.sql_buttons_frame,
            text="🔍 EXPLAIN",
            command=self.explain_query,
            font=ctk.CTkFont(size=14)
        )
        self.explain_btn.pack(side="left", padx=(0, 10))
        
        self.analyze_btn = ctk.CTkButton(
            self.sql_buttons_frame,
            text="📊 ANALYZE",
            command=self.analyze_query,
            font=ctk.CTkFont(size=14)
        )
        self.analyze_btn.pack(side="left")
        
        # 结果显示区域
        self.result_frame = ctk.CTkFrame(self.right_panel)
        self.result_frame.pack(fill="both", expand=True, padx=10, pady=10)
        
        ctk.CTkLabel(self.result_frame, text="执行结果", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=(10, 5))
        
        # 结果文本框
        self.result_textbox = ctk.CTkTextbox(
            self.result_frame,
            font=ctk.CTkFont(family="Consolas", size=14),
            state="disabled"
        )
        self.result_textbox.pack(fill="both", expand=True, padx=10, pady=5)
        
        # 状态栏
        self.status_frame = ctk.CTkFrame(self.right_panel)
        self.status_frame.pack(fill="x", padx=10, pady=(0, 10))
        
        self.status_label = ctk.CTkLabel(self.status_frame, text="就绪")
        self.status_label.pack(side="left", padx=10, pady=5)
        
        self.time_label = ctk.CTkLabel(self.status_frame, text="")
        self.time_label.pack(side="right", padx=10, pady=5)
        
        # 更新状态栏时间
        self.update_time()
    
    def setup_right_panel_standard(self):
        """设置标准 Tkinter 右侧SQL输入和结果面板"""
        # SQL输入区域
        self.sql_input_frame = ttk.LabelFrame(self.right_panel, text="SQL 查询", padding=10)
        self.sql_input_frame.pack(fill="x", pady=5)
        
        # SQL输入文本框
        self.sql_textbox = scrolledtext.ScrolledText(
            self.sql_input_frame, 
            height=8,
            font=("Consolas", 14),
            wrap=tk.WORD
        )
        self.sql_textbox.pack(fill="x", pady=5)
        
        # SQL操作按钮
        self.sql_buttons_frame = ttk.Frame(self.sql_input_frame)
        self.sql_buttons_frame.pack(fill="x", pady=10)
        
        self.execute_btn = ttk.Button(
            self.sql_buttons_frame,
            text="▶️ 执行 SQL",
            command=self.execute_sql
        )
        self.execute_btn.pack(side="left", padx=(0, 10))
        
        self.clear_btn = ttk.Button(
            self.sql_buttons_frame,
            text="🗑️ 清空",
            command=self.clear_sql
        )
        self.clear_btn.pack(side="left", padx=(0, 10))
        
        
        self.explain_btn = ttk.Button(
            self.sql_buttons_frame,
            text="🔍 EXPLAIN",
            command=self.explain_query
        )
        self.explain_btn.pack(side="left", padx=(0, 10))
        
        self.analyze_btn = ttk.Button(
            self.sql_buttons_frame,
            text="📊 ANALYZE",
            command=self.analyze_query
        )
        self.analyze_btn.pack(side="left")
        
        # 结果显示区域
        self.result_frame = ttk.LabelFrame(self.right_panel, text="执行结果", padding=10)
        self.result_frame.pack(fill="both", expand=True, pady=5)
        
        # 结果文本框
        self.result_textbox = scrolledtext.ScrolledText(
            self.result_frame,
            font=("Consolas", 14),
            state="disabled",
            wrap=tk.WORD
        )
        self.result_textbox.pack(fill="both", expand=True, pady=5)
        
        # 状态栏
        self.status_frame = ttk.Frame(self.right_panel)
        self.status_frame.pack(fill="x", pady=5)
        
        self.status_label = ttk.Label(self.status_frame, text="就绪")
        self.status_label.pack(side="left", padx=10, pady=5)
        
        self.time_label = ttk.Label(self.status_frame, text="")
        self.time_label.pack(side="right", padx=10, pady=5)
        
        # 更新状态栏时间
        self.update_time()
    
    def init_database_system(self):
        """初始化数据库系统"""
        try:
            self.status_label.configure(text="正在初始化数据库系统...")
            self.root.update()
            
            # 初始化系统管理器
            self.system_manager = SystemManager(base_data_dir='data')
            
            # 创建默认数据库
            if not os.path.exists('data/default'):
                self.system_manager.create_database('default')
            self.system_manager.use_database('default')
            
            # 【修改】不再需要CLI接口，直接使用SystemManager
            # self.cli_interface = CLIInterface(system_manager=self.system_manager)
            
            self.status_label.configure(text="数据库系统初始化完成")
            self.refresh_databases()
            self.refresh_tables()
            self.update_system_info()
            
        except Exception as e:
            self.status_label.configure(text=f"初始化失败: {str(e)}")
            self.log_result(f"❌ 系统初始化失败: {str(e)}")
    
    def refresh_databases(self):
        """刷新数据库列表"""
        try:
            # 清空现有列表
            if hasattr(self, 'db_listbox'):
                for widget in self.db_listbox.winfo_children():
                    widget.destroy()
            
            # 扫描数据库
            if self.system_manager:
                databases = []
                if os.path.exists('data'):
                    for item in os.listdir('data'):
                        if os.path.isdir(os.path.join('data', item)) and not item.startswith('.'):
                            databases.append(item)
                
                # 更新下拉框
                if hasattr(self, 'db_dropdown'):
                    self.db_dropdown.configure(values=databases)
                    if databases and self.system_manager.current_db_name in databases:
                        self.db_dropdown.set(self.system_manager.current_db_name)
                
                # 更新按钮列表（兼容旧版本）
                for db_name in databases:
                    if self.use_customtkinter and hasattr(self, 'db_listbox'):
                        db_btn = ctk.CTkButton(
                            self.db_listbox,
                            text=db_name,
                            command=lambda name=db_name: self.switch_database(name),
                            height=30
                        )
                        db_btn.pack(fill="x", pady=2)
                    elif hasattr(self, 'db_listbox'):
                        db_btn = ttk.Button(
                            self.db_listbox,
                            text=db_name,
                            command=lambda name=db_name: self.switch_database(name)
                        )
                        db_btn.pack(fill="x", pady=2)
                
                # 更新状态显示
                self.update_current_status()
                
                if databases:
                    self.log_result(f"✅ 发现 {len(databases)} 个数据库")
                else:
                    self.log_result("⚠️ 无可用数据库")
                    
        except Exception as e:
            self.log_result(f"❌ 刷新数据库列表失败: {str(e)}")
    
    def on_database_selected(self, selected_db):
        """数据库选择下拉框回调"""
        if selected_db and selected_db != "加载中...":
            self.switch_database(selected_db)
    
    def update_current_status(self):
        """更新当前状态显示"""
        try:
            if self.system_manager and self.system_manager.current_db_name:
                # 更新当前数据库标签
                if hasattr(self, 'current_db_label'):
                    self.current_db_label.configure(text=f"数据库: {self.system_manager.current_db_name}")
                
                # 更新表数量
                if hasattr(self, 'table_count_label'):
                    components = self.system_manager.get_current_components()
                    catalog_manager = components['catalog_manager']
                    table_count = len(catalog_manager.list_tables())
                    self.table_count_label.configure(text=f"表数量: {table_count}")
            else:
                if hasattr(self, 'current_db_label'):
                    self.current_db_label.configure(text="数据库: 未连接")
                if hasattr(self, 'table_count_label'):
                    self.table_count_label.configure(text="表数量: 0")
        except Exception as e:
            print(f"更新状态失败: {e}")
    
    
    def show_table_detail_window(self, table_name):
        """显示表详情窗口"""
        try:
            # 创建新窗口
            detail_window = ctk.CTkToplevel(self.root) if self.use_customtkinter else tk.Toplevel(self.root)
            detail_window.title(f"📋 表详情 - {table_name}")
            detail_window.geometry("1200x800")
            detail_window.minsize(1000, 700)
            
            # 设置窗口图标和样式
            try:
                detail_window.iconbitmap("icon.ico")
            except:
                pass
            
            # 创建主框架
            main_frame = ctk.CTkFrame(detail_window, corner_radius=15) if self.use_customtkinter else ttk.Frame(detail_window)
            main_frame.pack(fill="both", expand=True, padx=15, pady=15)
            
            # 创建顶部信息栏
            self.setup_table_header(main_frame, table_name)
            
            # 创建选项卡
            if self.use_customtkinter:
                tabview = ctk.CTkTabview(main_frame, corner_radius=10, border_width=2)
                tabview.pack(fill="both", expand=True, padx=15, pady=(10, 15))
                
                # 数据选项卡
                data_tab = tabview.add("📊 数据")
                tabview.set("📊 数据")  # 默认选中数据选项卡
                self.setup_data_tab(data_tab, table_name)
                
                # 结构选项卡
                structure_tab = tabview.add("🏗️ 结构")
                self.setup_structure_tab(structure_tab, table_name)
                
                # 索引选项卡
                indexes_tab = tabview.add("📈 索引")
                self.setup_indexes_tab(indexes_tab, table_name)
            else:
                # 标准Tkinter版本使用Notebook
                from tkinter import ttk
                notebook = ttk.Notebook(main_frame)
                notebook.pack(fill="both", expand=True, padx=15, pady=(10, 15))
                
                # 数据选项卡
                data_frame = ttk.Frame(notebook)
                notebook.add(data_frame, text="📊 数据")
                self.setup_data_tab(data_frame, table_name)
                
                # 结构选项卡
                structure_frame = ttk.Frame(notebook)
                notebook.add(structure_frame, text="🏗️ 结构")
                self.setup_structure_tab(structure_frame, table_name)
                
                # 索引选项卡
                indexes_frame = ttk.Frame(notebook)
                notebook.add(indexes_frame, text="📈 索引")
                self.setup_indexes_tab(indexes_frame, table_name)
            
            # 底部按钮栏
            self.setup_table_footer(main_frame, detail_window)
            
        except Exception as e:
            self.log_result(f"❌ 打开表详情失败: {str(e)}")
    
    def setup_table_header(self, parent, table_name):
        """设置表详情页面的头部信息"""
        try:
            # 获取表信息
            components = self.system_manager.get_current_components()
            catalog_manager = components['catalog_manager']
            table_info = catalog_manager.get_table(table_name)
            
            # 创建头部框架
            header_frame = ctk.CTkFrame(parent, height=80, corner_radius=10) if self.use_customtkinter else ttk.Frame(parent)
            header_frame.pack(fill="x", padx=15, pady=(15, 10))
            header_frame.pack_propagate(False)
            
            if self.use_customtkinter:
                # 左侧：表图标和名称
                left_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
                left_frame.pack(side="left", fill="y", padx=20, pady=15)
                
                # 表图标
                icon_label = ctk.CTkLabel(left_frame, text="📋", font=ctk.CTkFont(size=32))
                icon_label.pack(side="left", padx=(0, 15))
                
                # 表信息
                info_frame = ctk.CTkFrame(left_frame, fg_color="transparent")
                info_frame.pack(side="left", fill="y")
                
                # 表名
                name_label = ctk.CTkLabel(info_frame, text=table_name, 
                                        font=ctk.CTkFont(size=24, weight="bold"))
                name_label.pack(anchor="w")
                
                # 表描述
                desc_label = ctk.CTkLabel(info_frame, text="数据库表", 
                                        font=ctk.CTkFont(size=14), text_color="gray")
                desc_label.pack(anchor="w")
                
                # 右侧：统计信息
                right_frame = ctk.CTkFrame(header_frame, fg_color="transparent")
                right_frame.pack(side="right", fill="y", padx=20, pady=15)
                
                # 行数统计
                rows_label = ctk.CTkLabel(right_frame, text=f"📊 {table_info.row_count} 行", 
                                        font=ctk.CTkFont(size=16, weight="bold"))
                rows_label.pack(anchor="e")
                
                # 列数统计
                cols_label = ctk.CTkLabel(right_frame, text=f"📋 {len(table_info.columns)} 列", 
                                        font=ctk.CTkFont(size=14), text_color="gray")
                cols_label.pack(anchor="e")
                
                # 文件信息
                file_label = ctk.CTkLabel(right_frame, text=f"💾 {table_info.file_name}", 
                                        font=ctk.CTkFont(size=12), text_color="gray")
                file_label.pack(anchor="e")
            else:
                # 标准Tkinter版本
                left_frame = ttk.Frame(header_frame)
                left_frame.pack(side="left", fill="y", padx=20, pady=15)
                
                icon_label = ttk.Label(left_frame, text="📋", font=("Arial", 24))
                icon_label.pack(side="left", padx=(0, 15))
                
                info_frame = ttk.Frame(left_frame)
                info_frame.pack(side="left", fill="y")
                
                name_label = ttk.Label(info_frame, text=table_name, font=("Arial", 18, "bold"))
                name_label.pack(anchor="w")
                
                desc_label = ttk.Label(info_frame, text="数据库表", font=("Arial", 12), foreground="gray")
                desc_label.pack(anchor="w")
                
                right_frame = ttk.Frame(header_frame)
                right_frame.pack(side="right", fill="y", padx=20, pady=15)
                
                rows_label = ttk.Label(right_frame, text=f"📊 {table_info.row_count} 行", font=("Arial", 14, "bold"))
                rows_label.pack(anchor="e")
                
                cols_label = ttk.Label(right_frame, text=f"📋 {len(table_info.columns)} 列", font=("Arial", 12), foreground="gray")
                cols_label.pack(anchor="e")
                
                file_label = ttk.Label(right_frame, text=f"💾 {table_info.file_name}", font=("Arial", 10), foreground="gray")
                file_label.pack(anchor="e")
                
        except Exception as e:
            print(f"设置表头部失败: {e}")
    
    def setup_table_footer(self, parent, window):
        """设置表详情页面的底部按钮栏"""
        if self.use_customtkinter:
            footer_frame = ctk.CTkFrame(parent, height=60, corner_radius=10)
            footer_frame.pack(fill="x", padx=15, pady=(10, 15))
            footer_frame.pack_propagate(False)
            
            # 按钮容器
            button_frame = ctk.CTkFrame(footer_frame, fg_color="transparent")
            button_frame.pack(expand=True, fill="both", padx=20, pady=15)
            
            # 刷新按钮
            refresh_btn = ctk.CTkButton(button_frame, text="🔄 刷新", 
                                      command=lambda: self.refresh_table_detail(window),
                                      width=100, height=35,
                                      font=ctk.CTkFont(size=14))
            refresh_btn.pack(side="left", padx=(0, 10))
            
            # 关闭按钮
            close_btn = ctk.CTkButton(button_frame, text="❌ 关闭", 
                                    command=window.destroy,
                                    width=100, height=35,
                                    font=ctk.CTkFont(size=14),
                                    fg_color="#e74c3c", hover_color="#c0392b")
            close_btn.pack(side="right")
        else:
            # 标准Tkinter版本
            footer_frame = ttk.Frame(parent)
            footer_frame.pack(fill="x", padx=15, pady=(10, 15))
            
            button_frame = ttk.Frame(footer_frame)
            button_frame.pack(expand=True, fill="both", padx=20, pady=15)
            
            refresh_btn = ttk.Button(button_frame, text="🔄 刷新", 
                                   command=lambda: self.refresh_table_detail(window))
            refresh_btn.pack(side="left", padx=(0, 10))
            
            close_btn = ttk.Button(button_frame, text="❌ 关闭", command=window.destroy)
            close_btn.pack(side="right")
    
    def refresh_table_detail(self, window):
        """刷新表详情"""
        # 这里可以实现刷新逻辑
        self.log_result("🔄 表详情已刷新")
    
    def setup_data_tab(self, parent, table_name):
        """设置数据选项卡"""
        try:
            # 查询表数据
            sql = f"SELECT * FROM {table_name};"
            result = self.system_manager.execute_sql_statement(sql)
            
            if result.get('type') == 'SELECT':
                headers = result.get('headers', [])
                rows = result.get('rows', [])
                
                # 创建数据表格
                if self.use_customtkinter:
                    # 创建顶部统计信息栏
                    toolbar_frame = ctk.CTkFrame(parent, height=40, corner_radius=8)
                    toolbar_frame.pack(fill="x", padx=15, pady=(15, 10))
                    toolbar_frame.pack_propagate(False)
                    
                    # 统计信息
                    stats_frame = ctk.CTkFrame(toolbar_frame, fg_color="transparent")
                    stats_frame.pack(expand=True, fill="both", padx=15, pady=10)
                    
                    stats_label = ctk.CTkLabel(stats_frame, text=f"📊 共 {len(rows)} 行数据", 
                                             font=ctk.CTkFont(size=14, weight="bold"))
                    stats_label.pack(anchor="center")
                    
                    # 创建表格容器
                    table_container = ctk.CTkFrame(parent, corner_radius=10)
                    table_container.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                    
                    # 使用CTkScrollableFrame创建表格
                    table_frame = ctk.CTkScrollableFrame(table_container, corner_radius=8)
                    table_frame.pack(fill="both", expand=True, padx=10, pady=10)
                    
                    if headers and rows:
                        # 表头
                        header_frame = ctk.CTkFrame(table_frame, height=40, corner_radius=6)
                        header_frame.pack(fill="x", pady=(0, 8))
                        header_frame.pack_propagate(False)
                        
                        # 计算列宽 - 根据内容动态调整
                        col_widths = []
                        for i, header in enumerate(headers):
                            # 计算每列的最大宽度
                            max_width = len(header) * 8 + 20  # 基础宽度
                            for row in rows:
                                if i < len(row):
                                    cell_width = len(str(row[i])) * 8 + 20
                                    max_width = max(max_width, cell_width)
                            col_widths.append(min(max_width, 200))  # 最大200像素
                        
                        # 配置列权重
                        for i in range(len(headers)):
                            header_frame.grid_columnconfigure(i, weight=1)
                        
                        for i, (header, width) in enumerate(zip(headers, col_widths)):
                            header_label = ctk.CTkLabel(header_frame, text=header, 
                                                      font=ctk.CTkFont(size=13, weight="bold"),
                                                      width=width, height=30,
                                                      fg_color="#3498db", text_color="white",
                                                      corner_radius=4,
                                                      anchor="center")  # 居中对齐
                            header_label.grid(row=0, column=i, padx=3, pady=5, sticky="ew")
                        
                        # 数据行
                        for row_idx, row in enumerate(rows):
                            # 交替行颜色
                            row_color = "#f8f9fa" if row_idx % 2 == 0 else "#ffffff"
                            
                            row_frame = ctk.CTkFrame(table_frame, height=35, corner_radius=4, fg_color=row_color)
                            row_frame.pack(fill="x", pady=1)
                            row_frame.pack_propagate(False)
                            
                            # 配置行列权重
                            for i in range(len(headers)):
                                row_frame.grid_columnconfigure(i, weight=1)
                            
                            for col_idx, value in enumerate(row):
                                if col_idx < len(headers):
                                    # 截断过长的文本
                                    display_value = str(value)
                                    if len(display_value) > 25:
                                        display_value = display_value[:22] + "..."
                                    
                                    # 使用深色字体提高可读性
                                    cell_label = ctk.CTkLabel(row_frame, text=display_value, 
                                                            font=ctk.CTkFont(size=12),
                                                            width=col_widths[col_idx], height=25,
                                                            fg_color="transparent",
                                                            text_color="#2c3e50",  # 深色字体
                                                            anchor="center")  # 居中对齐
                                    cell_label.grid(row=0, column=col_idx, padx=3, pady=5, sticky="ew")
                    else:
                        # 无数据提示
                        no_data_label = ctk.CTkLabel(table_frame, text="📭 暂无数据", 
                                                   font=ctk.CTkFont(size=16), text_color="gray")
                        no_data_label.pack(expand=True, pady=50)
                else:
                    # 标准Tkinter版本
                    from tkinter import ttk
                    
                    # 创建统计信息栏
                    toolbar_frame = ttk.Frame(parent)
                    toolbar_frame.pack(fill="x", padx=15, pady=(15, 10))
                    
                    stats_label = ttk.Label(toolbar_frame, text=f"📊 共 {len(rows)} 行数据", font=("Arial", 12, "bold"))
                    stats_label.pack(anchor="center")
                    
                    # 创建Treeview表格
                    tree_frame = ttk.Frame(parent)
                    tree_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                    
                    tree = ttk.Treeview(tree_frame, columns=headers, show="headings", height=15)
                    
                    # 计算列宽
                    col_widths = []
                    for i, header in enumerate(headers):
                        max_width = len(header) * 8 + 20
                        for row in rows:
                            if i < len(row):
                                cell_width = len(str(row[i])) * 8 + 20
                                max_width = max(max_width, cell_width)
                        col_widths.append(min(max_width, 200))
                    
                    # 设置列标题和宽度
                    for header, width in zip(headers, col_widths):
                        tree.heading(header, text=header, anchor="center")
                        tree.column(header, width=width, anchor="center")
                    
                    # 插入数据
                    for row in rows:
                        tree.insert("", "end", values=row)
                    
                    # 添加滚动条
                    scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
                    tree.configure(yscrollcommand=scrollbar.set)
                    
                    tree.pack(side="left", fill="both", expand=True)
                    scrollbar.pack(side="right", fill="y")
            else:
                # 查询失败
                error_frame = ctk.CTkFrame(parent, corner_radius=10) if self.use_customtkinter else ttk.Frame(parent)
                error_frame.pack(fill="both", expand=True, padx=15, pady=15)
                
                error_label = ctk.CTkLabel(error_frame, text=f"❌ 查询失败: {result.get('message', '未知错误')}",
                                         font=ctk.CTkFont(size=16), text_color="#e74c3c") if self.use_customtkinter else ttk.Label(error_frame, text=f"❌ 查询失败: {result.get('message', '未知错误')}", font=("Arial", 14), foreground="red")
                error_label.pack(expand=True)
                
        except Exception as e:
            error_frame = ctk.CTkFrame(parent, corner_radius=10) if self.use_customtkinter else ttk.Frame(parent)
            error_frame.pack(fill="both", expand=True, padx=15, pady=15)
            
            error_label = ctk.CTkLabel(error_frame, text=f"❌ 加载数据失败: {str(e)}",
                                     font=ctk.CTkFont(size=16), text_color="#e74c3c") if self.use_customtkinter else ttk.Label(error_frame, text=f"❌ 加载数据失败: {str(e)}", font=("Arial", 14), foreground="red")
            error_label.pack(expand=True)
    
    def setup_structure_tab(self, parent, table_name):
        """设置结构选项卡"""
        try:
            components = self.system_manager.get_current_components()
            catalog_manager = components['catalog_manager']
            table_info = catalog_manager.get_table(table_name)
            
            # 创建结构信息显示
            if self.use_customtkinter:
                # 创建顶部信息卡片
                info_card = ctk.CTkFrame(parent, height=100, corner_radius=10)
                info_card.pack(fill="x", padx=15, pady=(15, 10))
                info_card.pack_propagate(False)
                
                # 表基本信息
                info_frame = ctk.CTkFrame(info_card, fg_color="transparent")
                info_frame.pack(expand=True, fill="both", padx=20, pady=15)
                
                # 表名和描述
                name_frame = ctk.CTkFrame(info_frame, fg_color="transparent")
                name_frame.pack(side="left", fill="y")
                
                name_label = ctk.CTkLabel(name_frame, text=f"📋 {table_name}", 
                                        font=ctk.CTkFont(size=18, weight="bold"))
                name_label.pack(anchor="w")
                
                desc_label = ctk.CTkLabel(name_frame, text="表结构信息", 
                                        font=ctk.CTkFont(size=12), text_color="gray")
                desc_label.pack(anchor="w")
                
                # 统计信息
                stats_frame = ctk.CTkFrame(info_frame, fg_color="transparent")
                stats_frame.pack(side="right", fill="y")
                
                stats_items = [
                    (f"📊 {table_info.row_count}", "行数据"),
                    (f"📋 {len(table_info.columns)}", "列"),
                    (f"💾 {table_info.file_name}", "文件"),
                    (f"📄 {table_info.page_count}", "页")
                ]
                
                for i, (value, label) in enumerate(stats_items):
                    stat_frame = ctk.CTkFrame(stats_frame, fg_color="transparent")
                    stat_frame.pack(side="left", padx=(0, 20))
                    
                    value_label = ctk.CTkLabel(stat_frame, text=value, 
                                             font=ctk.CTkFont(size=14, weight="bold"))
                    value_label.pack(anchor="e")
                    
                    label_label = ctk.CTkLabel(stat_frame, text=label, 
                                             font=ctk.CTkFont(size=10), text_color="gray")
                    label_label.pack(anchor="e")
                
                # 创建列信息表格
                table_container = ctk.CTkFrame(parent, corner_radius=10)
                table_container.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                
                structure_frame = ctk.CTkScrollableFrame(table_container, corner_radius=8)
                structure_frame.pack(fill="both", expand=True, padx=10, pady=10)
                
                # 表头
                header_frame = ctk.CTkFrame(structure_frame, height=40, corner_radius=6)
                header_frame.pack(fill="x", pady=(0, 8))
                header_frame.pack_propagate(False)
                
                headers = ["列名", "数据类型", "主键", "非空", "默认值"]
                col_widths = [150, 120, 80, 80, 120]
                
                for i, (header, width) in enumerate(zip(headers, col_widths)):
                    header_label = ctk.CTkLabel(header_frame, text=header, 
                                              font=ctk.CTkFont(size=13, weight="bold"),
                                              width=width, height=30,
                                              fg_color="#2c3e50", text_color="white",
                                              corner_radius=4)
                    header_label.grid(row=0, column=i, padx=2, pady=5, sticky="ew")
                
                # 列信息
                for col_idx, col in enumerate(table_info.columns):
                    # 交替行颜色
                    row_color = "#f8f9fa" if col_idx % 2 == 0 else "#ffffff"
                    
                    row_frame = ctk.CTkFrame(structure_frame, height=35, corner_radius=4, fg_color=row_color)
                    row_frame.pack(fill="x", pady=1)
                    row_frame.pack_propagate(False)
                    
                    col_data = [
                        col.column_name,
                        col.data_type,
                        "✅ 是" if getattr(col, 'primary_key', False) else "❌ 否",
                        "✅ 是" if col.not_null else "❌ 否",
                        str(col.default) if col.default else "无"
                    ]
                    
                    for col_idx, (data, width) in enumerate(zip(col_data, col_widths)):
                        # 根据数据类型设置颜色
                        if col_idx == 0:  # 列名
                            text_color = "#2c3e50"
                        elif col_idx == 2:  # 主键
                            text_color = "#e74c3c" if "是" in data else "gray"
                        elif col_idx == 3:  # 非空
                            text_color = "#27ae60" if "是" in data else "gray"
                        else:
                            text_color = "black"
                        
                        cell_label = ctk.CTkLabel(row_frame, text=str(data), 
                                                font=ctk.CTkFont(size=12),
                                                width=width, height=25,
                                                fg_color="transparent",
                                                text_color=text_color,
                                                anchor="w")
                        cell_label.grid(row=0, column=col_idx, padx=2, pady=5, sticky="ew")
            else:
                # 标准Tkinter版本
                from tkinter import ttk
                
                # 创建信息框架
                info_frame = ttk.Frame(parent)
                info_frame.pack(fill="x", padx=15, pady=(15, 10))
                
                # 表名
                name_label = ttk.Label(info_frame, text=f"📋 {table_name}", font=("Arial", 16, "bold"))
                name_label.pack(anchor="w")
                
                # 统计信息
                stats_text = f"📊 {table_info.row_count} 行 | 📋 {len(table_info.columns)} 列 | 💾 {table_info.file_name}"
                stats_label = ttk.Label(info_frame, text=stats_text, font=("Arial", 12), foreground="gray")
                stats_label.pack(anchor="w")
                
                # 创建Treeview表格
                tree_frame = ttk.Frame(parent)
                tree_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                
                tree = ttk.Treeview(tree_frame, columns=["列名", "数据类型", "主键", "非空", "默认值"], show="headings", height=15)
                
                # 设置列标题
                for header in ["列名", "数据类型", "主键", "非空", "默认值"]:
                    tree.heading(header, text=header)
                    tree.column(header, width=120)
                
                # 插入列信息
                for col in table_info.columns:
                    tree.insert("", "end", values=[
                        col.column_name,
                        col.data_type,
                        "是" if getattr(col, 'primary_key', False) else "否",
                        "是" if col.not_null else "否",
                        str(col.default) if col.default else "无"
                    ])
                
                tree.pack(side="left", fill="both", expand=True)
                
                # 添加滚动条
                scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
                tree.configure(yscrollcommand=scrollbar.set)
                scrollbar.pack(side="right", fill="y")
                
        except Exception as e:
            error_frame = ctk.CTkFrame(parent, corner_radius=10) if self.use_customtkinter else ttk.Frame(parent)
            error_frame.pack(fill="both", expand=True, padx=15, pady=15)
            
            error_label = ctk.CTkLabel(error_frame, text=f"❌ 加载结构失败: {str(e)}",
                                     font=ctk.CTkFont(size=16), text_color="#e74c3c") if self.use_customtkinter else ttk.Label(error_frame, text=f"❌ 加载结构失败: {str(e)}", font=("Arial", 14), foreground="red")
            error_label.pack(expand=True)
    
    def setup_indexes_tab(self, parent, table_name):
        """设置索引选项卡"""
        try:
            components = self.system_manager.get_current_components()
            catalog_manager = components['catalog_manager']
            table_info = catalog_manager.get_table(table_name)
            
            if self.use_customtkinter:
                # 创建顶部信息卡片
                info_card = ctk.CTkFrame(parent, height=80, corner_radius=10)
                info_card.pack(fill="x", padx=15, pady=(15, 10))
                info_card.pack_propagate(False)
                
                # 索引统计信息
                info_frame = ctk.CTkFrame(info_card, fg_color="transparent")
                info_frame.pack(expand=True, fill="both", padx=20, pady=15)
                
                # 左侧：索引图标和描述
                left_frame = ctk.CTkFrame(info_frame, fg_color="transparent")
                left_frame.pack(side="left", fill="y")
                
                icon_label = ctk.CTkLabel(left_frame, text="📈", font=ctk.CTkFont(size=24))
                icon_label.pack(side="left", padx=(0, 15))
                
                desc_frame = ctk.CTkFrame(left_frame, fg_color="transparent")
                desc_frame.pack(side="left", fill="y")
                
                title_label = ctk.CTkLabel(desc_frame, text="索引信息", 
                                         font=ctk.CTkFont(size=16, weight="bold"))
                title_label.pack(anchor="w")
                
                count_label = ctk.CTkLabel(desc_frame, text=f"共 {len(table_info.indexes)} 个索引", 
                                         font=ctk.CTkFont(size=12), text_color="gray")
                count_label.pack(anchor="w")
                
                # 右侧：统计信息
                right_frame = ctk.CTkFrame(info_frame, fg_color="transparent")
                right_frame.pack(side="right", fill="y")
                
                if table_info.indexes:
                    # 统计不同类型的索引
                    unique_count = sum(1 for idx in table_info.indexes.values() if getattr(idx, 'is_unique', False))
                    btree_count = sum(1 for idx in table_info.indexes.values() if getattr(idx, 'index_type', 'BTREE') == 'BTREE')
                    
                    stats_items = [
                        (f"🔑 {unique_count}", "唯一索引"),
                        (f"🌳 {btree_count}", "B树索引"),
                        (f"📊 {len(table_info.indexes)}", "总索引")
                    ]
                    
                    for value, label in stats_items:
                        stat_frame = ctk.CTkFrame(right_frame, fg_color="transparent")
                        stat_frame.pack(side="left", padx=(0, 15))
                        
                        value_label = ctk.CTkLabel(stat_frame, text=value, 
                                                 font=ctk.CTkFont(size=14, weight="bold"))
                        value_label.pack(anchor="e")
                        
                        label_label = ctk.CTkLabel(stat_frame, text=label, 
                                                 font=ctk.CTkFont(size=10), text_color="gray")
                        label_label.pack(anchor="e")
                else:
                    no_index_label = ctk.CTkLabel(right_frame, text="📭 暂无索引", 
                                                font=ctk.CTkFont(size=14), text_color="gray")
                    no_index_label.pack(anchor="e")
                
                # 创建索引列表
                if table_info.indexes:
                    table_container = ctk.CTkFrame(parent, corner_radius=10)
                    table_container.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                    
                    indexes_frame = ctk.CTkScrollableFrame(table_container, corner_radius=8)
                    indexes_frame.pack(fill="both", expand=True, padx=10, pady=10)
                    
                    # 表头
                    header_frame = ctk.CTkFrame(indexes_frame, height=40, corner_radius=6)
                    header_frame.pack(fill="x", pady=(0, 8))
                    header_frame.pack_propagate(False)
                    
                    headers = ["索引名", "类型", "列名", "唯一性", "状态"]
                    col_widths = [200, 100, 200, 100, 100]
                    
                    for i, (header, width) in enumerate(zip(headers, col_widths)):
                        header_label = ctk.CTkLabel(header_frame, text=header, 
                                                  font=ctk.CTkFont(size=13, weight="bold"),
                                                  width=width, height=30,
                                                  fg_color="#8e44ad", text_color="white",
                                                  corner_radius=4)
                        header_label.grid(row=0, column=i, padx=2, pady=5, sticky="ew")
                    
                    # 索引信息
                    for idx_idx, (index_name, index_info) in enumerate(table_info.indexes.items()):
                        # 交替行颜色
                        row_color = "#f8f9fa" if idx_idx % 2 == 0 else "#ffffff"
                        
                        row_frame = ctk.CTkFrame(indexes_frame, height=35, corner_radius=4, fg_color=row_color)
                        row_frame.pack(fill="x", pady=1)
                        row_frame.pack_propagate(False)
                        
                        index_data = [
                            index_name,
                            getattr(index_info, 'index_type', 'BTREE'),
                            ', '.join(getattr(index_info, 'column_names', [])),
                            "✅ 是" if getattr(index_info, 'is_unique', False) else "❌ 否",
                            "🟢 活跃"
                        ]
                        
                        for col_idx, (data, width) in enumerate(zip(index_data, col_widths)):
                            # 根据数据类型设置颜色
                            if col_idx == 0:  # 索引名
                                text_color = "#8e44ad"
                            elif col_idx == 3:  # 唯一性
                                text_color = "#e74c3c" if "是" in data else "gray"
                            elif col_idx == 4:  # 状态
                                text_color = "#27ae60"
                            else:
                                text_color = "black"
                            
                            cell_label = ctk.CTkLabel(row_frame, text=str(data), 
                                                    font=ctk.CTkFont(size=12),
                                                    width=width, height=25,
                                                    fg_color="transparent",
                                                    text_color=text_color,
                                                    anchor="w")
                            cell_label.grid(row=0, column=col_idx, padx=2, pady=5, sticky="ew")
                else:
                    # 无索引提示
                    no_index_frame = ctk.CTkFrame(parent, corner_radius=10)
                    no_index_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                    
                    no_index_label = ctk.CTkLabel(no_index_frame, text="📭 该表暂无索引", 
                                                font=ctk.CTkFont(size=18), text_color="gray")
                    no_index_label.pack(expand=True, pady=50)
                    
                    hint_label = ctk.CTkLabel(no_index_frame, text="💡 提示：可以通过 CREATE INDEX 语句创建索引以提高查询性能", 
                                            font=ctk.CTkFont(size=12), text_color="gray")
                    hint_label.pack(pady=(0, 50))
            else:
                # 标准Tkinter版本
                from tkinter import ttk
                
                # 创建信息框架
                info_frame = ttk.Frame(parent)
                info_frame.pack(fill="x", padx=15, pady=(15, 10))
                
                # 标题
                title_label = ttk.Label(info_frame, text=f"📈 索引信息 - {table_name}", font=("Arial", 16, "bold"))
                title_label.pack(anchor="w")
                
                # 统计信息
                stats_text = f"共 {len(table_info.indexes)} 个索引"
                stats_label = ttk.Label(info_frame, text=stats_text, font=("Arial", 12), foreground="gray")
                stats_label.pack(anchor="w")
                
                if table_info.indexes:
                    # 创建Treeview表格
                    tree_frame = ttk.Frame(parent)
                    tree_frame.pack(fill="both", expand=True, padx=15, pady=(0, 15))
                    
                    tree = ttk.Treeview(tree_frame, columns=["索引名", "类型", "列名", "唯一性"], show="headings", height=15)
                    
                    # 设置列标题
                    for header in ["索引名", "类型", "列名", "唯一性"]:
                        tree.heading(header, text=header)
                        tree.column(header, width=150)
                    
                    # 插入索引信息
                    for index_name, index_info in table_info.indexes.items():
                        tree.insert("", "end", values=[
                            index_name,
                            getattr(index_info, 'index_type', 'BTREE'),
                            ', '.join(getattr(index_info, 'column_names', [])),
                            "是" if getattr(index_info, 'is_unique', False) else "否"
                        ])
                    
                    tree.pack(side="left", fill="both", expand=True)
                    
                    # 添加滚动条
                    scrollbar = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
                    tree.configure(yscrollcommand=scrollbar.set)
                    scrollbar.pack(side="right", fill="y")
                else:
                    # 无索引提示
                    no_index_label = ttk.Label(parent, text="📭 该表暂无索引", font=("Arial", 14), foreground="gray")
                    no_index_label.pack(expand=True, pady=50)
                    
        except Exception as e:
            error_frame = ctk.CTkFrame(parent, corner_radius=10) if self.use_customtkinter else ttk.Frame(parent)
            error_frame.pack(fill="both", expand=True, padx=15, pady=15)
            
            error_label = ctk.CTkLabel(error_frame, text=f"❌ 加载索引失败: {str(e)}",
                                     font=ctk.CTkFont(size=16), text_color="#e74c3c") if self.use_customtkinter else ttk.Label(error_frame, text=f"❌ 加载索引失败: {str(e)}", font=("Arial", 14), foreground="red")
            error_label.pack(expand=True)
    
    def refresh_tables(self):
        """刷新表列表"""
        try:
            # 清空现有列表
            if hasattr(self, 'table_listbox'):
                for widget in self.table_listbox.winfo_children():
                    widget.destroy()
            
            if hasattr(self, 'table_list_frame'):
                for widget in self.table_list_frame.winfo_children():
                    widget.destroy()
            
            if self.system_manager and self.system_manager.current_db_name:
                # 获取当前数据库的组件
                components = self.system_manager.get_current_components()
                catalog_manager = components['catalog_manager']
                
                tables = catalog_manager.list_tables()
                
                if tables:
                    # 使用新的表列表框架
                    target_frame = self.table_list_frame if hasattr(self, 'table_list_frame') else self.table_listbox
                    
                    for table_name in tables:
                        if self.use_customtkinter:
                            table_btn = ctk.CTkButton(
                                target_frame,
                                text=f"📋 {table_name}",
                                command=lambda name=table_name: self.show_table_detail_window(name),
                                height=35,
                                anchor="w",
                                font=ctk.CTkFont(size=12)
                            )
                        else:
                            table_btn = ttk.Button(
                                target_frame,
                                text=f"📋 {table_name}",
                                command=lambda name=table_name: self.show_table_detail_window(name)
                            )
                        table_btn.pack(fill="x", pady=2)
                    
                    # 更新状态显示
                    self.update_current_status()
                    self.log_result(f"✅ 发现 {len(tables)} 个表")
                else:
                    target_frame = self.table_list_frame if hasattr(self, 'table_list_frame') else self.table_listbox
                    if self.use_customtkinter:
                        no_tables_label = ctk.CTkLabel(target_frame, text="📭 暂无表", font=ctk.CTkFont(size=12))
                    else:
                        no_tables_label = ttk.Label(target_frame, text="📭 暂无表")
                    no_tables_label.pack(pady=10)
            else:
                target_frame = self.table_list_frame if hasattr(self, 'table_list_frame') else self.table_listbox
                if self.use_customtkinter:
                    no_db_label = ctk.CTkLabel(target_frame, text="⚠️ 请先选择数据库", font=ctk.CTkFont(size=12))
                else:
                    no_db_label = ttk.Label(target_frame, text="⚠️ 请先选择数据库")
                no_db_label.pack(pady=10)
                
        except Exception as e:
            self.log_result(f"❌ 刷新表列表失败: {str(e)}")
    
    def switch_database(self, db_name: str):
        """切换数据库"""
        try:
            if self.system_manager:
                self.system_manager.use_database(db_name)
                
                # 更新下拉框选择
                if hasattr(self, 'db_dropdown'):
                    self.db_dropdown.set(db_name)
                
                # 更新状态显示
                self.update_current_status()
                
                # 刷新表列表和系统信息
                self.refresh_tables()
                self.update_system_info()
                self.log_result(f"✅ 已切换到数据库: {db_name}")
        except Exception as e:
            self.log_result(f"❌ 切换数据库失败: {str(e)}")
    
    def create_database(self):
        """创建新数据库"""
        try:
            # 简单的输入对话框
            if self.use_customtkinter:
                dialog = ctk.CTkInputDialog(text="请输入数据库名称:", title="创建数据库")
                db_name = dialog.get_input()
            else:
                from tkinter import simpledialog
                db_name = simpledialog.askstring("创建数据库", "请输入数据库名称:")
            
            if db_name and self.system_manager:
                self.system_manager.create_database(db_name)
                self.refresh_databases()
                self.update_system_info()
                self.log_result(f"✅ 数据库 '{db_name}' 创建成功")
        except Exception as e:
            self.log_result(f"❌ 创建数据库失败: {str(e)}")
    
    def show_table_info(self, table_name: str):
        """显示表信息"""
        try:
            if self.system_manager:
                components = self.system_manager.get_current_components()
                catalog_manager = components['catalog_manager']
                
                table_info = catalog_manager.get_table(table_name)
                
                # 使用 rich 格式化表信息
                columns_info = []
                for col in table_info.columns:
                    columns_info.append({
                        'name': col.column_name,
                        'type': col.data_type,
                        'primary_key': False,  # 简化处理
                        'not_null': False
                    })
                
                formatted_info = self.rich_display.format_table_schema(table_name, columns_info)
                self.log_result(formatted_info)
        except Exception as e:
            self.log_result(f"❌ 获取表信息失败: {str(e)}")
    
    def update_system_info(self):
        """更新系统信息显示"""
        try:
            if self.system_manager and self.system_manager.current_db_name:
                components = self.system_manager.get_current_components()
                catalog_manager = components['catalog_manager']
                
                # 获取系统统计信息
                table_count = len(catalog_manager.list_tables())
                db_name = self.system_manager.current_db_name
                
                # 获取数据库大小（简化计算）
                db_path = f"data/{db_name}"
                if os.path.exists(db_path):
                    total_size = sum(os.path.getsize(os.path.join(dirpath, filename))
                                   for dirpath, dirnames, filenames in os.walk(db_path)
                                   for filename in filenames)
                    size_mb = total_size / (1024 * 1024)
                else:
                    size_mb = 0
                
                system_info = f"""数据库: {db_name}
表数量: {table_count}
大小: {size_mb:.2f} MB
状态: 运行中
版本: AODSQL 1.0.0"""
                
                self.system_info_text.delete("1.0", "end")
                self.system_info_text.insert("1.0", system_info)
            else:
                self.system_info_text.delete("1.0", "end")
                self.system_info_text.insert("1.0", "未连接数据库")
        except Exception as e:
            self.system_info_text.delete("1.0", "end")
            self.system_info_text.insert("1.0", f"系统信息获取失败: {str(e)}")
    
    def show_triggers(self):
        """显示触发器信息"""
        try:
            if not self.system_manager or not self.system_manager.current_db_name:
                self.log_result("❌ 请先选择数据库")
                return
            
            # 执行SHOW TRIGGERS命令
            result = self.system_manager.execute_sql_statement("SHOW TRIGGERS")
            
            if result.get('type') == 'ERROR':
                self.log_result(f"❌ 获取触发器信息失败: {result.get('message')}")
            elif result.get('type') == 'SELECT':
                if result.get('rows'):
                    formatted = self.rich_display.format_select_result(
                        headers=result.get('headers', []),
                        rows=result.get('rows', [])
                    )
                    self.log_result(formatted)
                else:
                    self.log_result("📋 当前数据库中没有触发器")
            else:
                self.log_result("📋 触发器功能暂未实现")
                
        except Exception as e:
            self.log_result(f"❌ 查看触发器失败: {str(e)}")
    
    def show_views(self):
        """显示视图信息"""
        try:
            if not self.system_manager or not self.system_manager.current_db_name:
                self.log_result("❌ 请先选择数据库")
                return
            
            # 执行SHOW VIEWS命令
            result = self.system_manager.execute_sql_statement("SHOW VIEWS")
            
            if result.get('type') == 'ERROR':
                self.log_result(f"❌ 获取视图信息失败: {result.get('message')}")
            elif result.get('type') == 'SELECT':
                if result.get('rows'):
                    formatted = self.rich_display.format_select_result(
                        headers=result.get('headers', []),
                        rows=result.get('rows', [])
                    )
                    self.log_result(formatted)
                else:
                    self.log_result("👁️ 当前数据库中没有视图")
            else:
                self.log_result("👁️ 视图功能暂未实现")
                
        except Exception as e:
            self.log_result(f"❌ 查看视图失败: {str(e)}")
    
    def show_indexes(self):
        """显示索引信息"""
        try:
            if not self.system_manager or not self.system_manager.current_db_name:
                self.log_result("❌ 请先选择数据库")
                return
            
            components = self.system_manager.get_current_components()
            catalog_manager = components['catalog_manager']
            
            # 获取所有表的索引信息
            tables = catalog_manager.list_tables()
            if not tables:
                self.log_result("📊 当前数据库中没有表")
                return
            
            index_info = []
            for table_name in tables:
                table_info = catalog_manager.get_table(table_name)
                if hasattr(table_info, 'indexes') and table_info.indexes:
                    for index_name, index_info_obj in table_info.indexes.items():
                        index_info.append([
                            table_name,
                            index_name,
                            ', '.join(index_info_obj.columns) if hasattr(index_info_obj, 'columns') else 'N/A',
                            'B+树' if hasattr(index_info_obj, 'type') else 'N/A'
                        ])
            
            if index_info:
                formatted = self.rich_display.format_select_result(
                    headers=['表名', '索引名', '列', '类型'],
                    rows=index_info
                )
                self.log_result(formatted)
            else:
                self.log_result("📊 当前数据库中没有索引")
                
        except Exception as e:
            self.log_result(f"❌ 查看索引失败: {str(e)}")
    
    def show_performance(self):
        """显示性能监控信息"""
        try:
            if not self.system_manager or not self.system_manager.current_db_name:
                self.log_result("❌ 请先选择数据库")
                return
            
            components = self.system_manager.get_current_components()
            catalog_manager = components['catalog_manager']
            
            # 收集性能统计信息
            tables = catalog_manager.list_tables()
            performance_data = []
            
            for table_name in tables:
                try:
                    table_info = catalog_manager.get_table(table_name)
                    row_count = getattr(table_info, 'row_count', 0)
                    page_count = getattr(table_info, 'page_count', 0)
                    
                    performance_data.append([
                        table_name,
                        str(row_count),
                        str(page_count),
                        f"{page_count * 4:.2f} KB" if page_count else "0 KB"
                    ])
                except:
                    performance_data.append([table_name, "N/A", "N/A", "N/A"])
            
            if performance_data:
                formatted = self.rich_display.format_select_result(
                    headers=['表名', '行数', '页数', '大小'],
                    rows=performance_data
                )
                self.log_result(formatted)
            else:
                self.log_result("⚡ 没有性能数据可显示")
                
        except Exception as e:
            self.log_result(f"❌ 性能监控失败: {str(e)}")
    
    def execute_sql(self):
        """执行SQL语句"""
        sql_text = self.sql_textbox.get("1.0", "end-1c").strip()
        
        if not sql_text:
            self.log_result("❌ 请输入SQL语句")
            return
        
        try:
            self.status_label.configure(text="正在执行SQL...")
            self.execute_btn.configure(state="disabled")
            self.root.update()
            
            # 在新线程中执行SQL，避免界面冻结
            self.sql_queue = queue.Queue()
            thread = threading.Thread(target=self._execute_sql_thread_direct, args=(sql_text,))
            thread.daemon = True
            thread.start()
            
            # 检查结果
            self.root.after(100, self._check_sql_result)
            
        except Exception as e:
            self.log_result(f"❌ 执行SQL失败: {str(e)}")
            self.status_label.configure(text="就绪")
            self.execute_btn.configure(state="normal")
    
    def _execute_sql_thread_direct(self, sql_text: str):
        """在后台线程中直接执行SQL（新方法），返回结构化数据"""
        try:
            # 直接调用SystemManager的execute_sql_statement方法
            result_data = self.system_manager.execute_sql_statement(sql_text)
            
            # 使用 rich 格式化结构化结果
            formatted_result = self._format_structured_result(result_data)
            
            self.sql_queue.put(("success", formatted_result))
            
            # 【新功能】如果DDL操作成功（如建表、删表），自动刷新左侧表列表
            if result_data.get('type') in ['DDL', 'CREATE_TABLE', 'DROP_TABLE']:
                self.root.after(0, self.refresh_tables)
                
        except Exception as e:
            # 捕获执行期间的任何异常
            import traceback
            error_message = f"执行时发生内部错误: {str(e)}\n{traceback.format_exc()}"
            formatted_error = self.rich_display.format_error(error_message)
            self.sql_queue.put(("error", formatted_error))
    
    def _format_structured_result(self, result_data: Dict[str, Any]) -> str:
        """根据返回的结构化数据，调用 RichDisplayManager 进行格式化"""
        result_type = result_data.get('type', 'UNKNOWN').upper()
        
        if result_type == 'SELECT':
            return self.rich_display.format_select_result(
                headers=result_data.get('headers', []),
                rows=result_data.get('rows', [])
            )
        elif result_type in ['DML', 'INSERT', 'UPDATE', 'DELETE']:
            return self.rich_display.format_dml_result(result_data.get('message', '操作完成'))
        elif result_type in ['DDL', 'CREATE_TABLE', 'DROP_TABLE', 'SHOW_TABLES']:
            return self.rich_display.format_ddl_result(result_data.get('message', '操作成功'))
        elif result_type == 'ERROR':
            return self.rich_display.format_error(result_data.get('message', '未知错误'))
        else:
            # 对于其他未知类型，以通用方式显示
            return self.rich_display.format_general_string(str(result_data))
    
    def _check_sql_result(self):
        """检查SQL执行结果 (逻辑简化)"""
        try:
            if not self.sql_queue.empty():
                result_type, result = self.sql_queue.get()
                
                # 不再需要区分 success 和 error，因为错误也已经被格式化了
                self.log_result(result)
                
                self.status_label.configure(text="就绪")
                self.execute_btn.configure(state="normal")
            else:
                self.root.after(100, self._check_sql_result)
        except Exception as e:
            self.log_result(f"❌ 检查结果失败: {str(e)}")
            self.status_label.configure(text="就绪")
            self.execute_btn.configure(state="normal")
    
    def clear_sql(self):
        """清空SQL输入"""
        self.sql_textbox.delete("1.0", "end")
    
    
    def explain_query(self):
        """显示查询执行计划"""
        sql_text = self.sql_textbox.get("1.0", "end-1c").strip()
        if not sql_text:
            self.log_result("❌ 请输入SQL语句")
            return
        
        try:
            self.status_label.configure(text="正在分析执行计划...")
            self.root.update()
            
            # 在新线程中执行EXPLAIN
            self.sql_queue = queue.Queue()
            thread = threading.Thread(target=self._explain_query_thread, args=(sql_text,))
            thread.daemon = True
            thread.start()
            
            self.root.after(100, self._check_sql_result)
            
        except Exception as e:
            self.log_result(f"❌ EXPLAIN失败: {str(e)}")
            self.status_label.configure(text="就绪")
    
    def analyze_query(self):
        """执行EXPLAIN ANALYZE"""
        sql_text = self.sql_textbox.get("1.0", "end-1c").strip()
        if not sql_text:
            self.log_result("❌ 请输入SQL语句")
            return
        
        try:
            self.status_label.configure(text="正在执行EXPLAIN ANALYZE...")
            self.root.update()
            
            # 在新线程中执行ANALYZE
            self.sql_queue = queue.Queue()
            thread = threading.Thread(target=self._analyze_query_thread, args=(sql_text,))
            thread.daemon = True
            thread.start()
            
            self.root.after(100, self._check_sql_result)
            
        except Exception as e:
            self.log_result(f"❌ ANALYZE失败: {str(e)}")
            self.status_label.configure(text="就绪")
    
    def _explain_query_thread(self, sql_text: str):
        """在后台线程中执行EXPLAIN"""
        try:
            # 获取当前数据库的组件
            components = self.system_manager.get_current_components()
            sql_interpreter = components['sql_interpreter']
            catalog_manager = components['catalog_manager']
            storage_engine = components['storage_engine']
            
            # 解析SQL
            result = sql_interpreter.interpret(sql_text)
            if result["status"] == "error":
                self.sql_queue.put(("error", f"❌ 编译失败: {result['message']}"))
                return
            
            # 转换为物理计划
            from cli.plan_converter import PlanConverter
            plan_converter = PlanConverter(storage_engine, catalog_manager)
            physical_plan = plan_converter.convert_to_physical_plan(result["operator_tree"])
            
            if not physical_plan:
                self.sql_queue.put(("error", "❌ 无法生成物理执行计划"))
                return
            
            # 格式化执行计划
            formatted_plan = self.rich_display.format_execution_plan({
                'type': 'PhysicalPlan',
                'properties': {'sql': sql_text},
                'children': [self._physical_plan_to_dict(physical_plan)]
            })
            
            self.sql_queue.put(("success", formatted_plan))
            
        except Exception as e:
            import traceback
            error_message = f"EXPLAIN执行失败: {str(e)}\n{traceback.format_exc()}"
            self.sql_queue.put(("error", error_message))
    
    def _analyze_query_thread(self, sql_text: str):
        """在后台线程中执行EXPLAIN ANALYZE"""
        try:
            # 获取当前数据库的组件
            components = self.system_manager.get_current_components()
            sql_interpreter = components['sql_interpreter']
            catalog_manager = components['catalog_manager']
            storage_engine = components['storage_engine']
            executor = components['executor']
            transaction_manager = components['transaction_manager']
            
            # 解析SQL
            result = sql_interpreter.interpret(sql_text)
            if result["status"] == "error":
                self.sql_queue.put(("error", f"❌ 编译失败: {result['message']}"))
                return
            
            # 转换为物理计划
            from cli.plan_converter import PlanConverter
            plan_converter = PlanConverter(storage_engine, catalog_manager)
            physical_plan = plan_converter.convert_to_physical_plan(result["operator_tree"])
            
            if not physical_plan:
                self.sql_queue.put(("error", "❌ 无法生成物理执行计划"))
                return
            
            # 执行查询以收集性能数据
            import time
            start_time = time.time()
            
            if transaction_manager:
                transaction = transaction_manager.begin()
                try:
                    execution_result = executor.execute_plan(physical_plan, transaction)
                    transaction_manager.commit(transaction)
                except Exception as e:
                    transaction_manager.abort(transaction)
                    raise e
            else:
                from src.engine.transaction.transaction import Transaction, IsolationLevel
                transaction = Transaction(1, IsolationLevel.READ_COMMITTED)
                execution_result = executor.execute_plan(physical_plan, transaction)
            
            execution_time = time.time() - start_time
            
            # 格式化带分析的执行计划
            analysis_info = f"执行时间: {execution_time:.3f}秒\n处理行数: {len(execution_result) if execution_result else 0}"
            
            formatted_plan = self.rich_display.format_execution_plan({
                'type': 'PhysicalPlanWithAnalysis',
                'properties': {'sql': sql_text, 'analysis': analysis_info},
                'children': [self._physical_plan_to_dict(physical_plan)]
            })
            
            self.sql_queue.put(("success", formatted_plan))
            
        except Exception as e:
            import traceback
            error_message = f"ANALYZE执行失败: {str(e)}\n{traceback.format_exc()}"
            self.sql_queue.put(("error", error_message))
    
    def _physical_plan_to_dict(self, plan):
        """将物理计划转换为字典格式用于显示"""
        if hasattr(plan, '__class__'):
            plan_dict = {
                'type': plan.__class__.__name__,
                'properties': {}
            }
            
            # 添加重要属性
            important_attrs = ['table_name', 'condition', 'columns', 'sort_key_info']
            for attr in important_attrs:
                if hasattr(plan, attr):
                    plan_dict['properties'][attr] = getattr(plan, attr)
            
            # 添加子节点
            children = []
            if hasattr(plan, 'child') and plan.child:
                children.append(self._physical_plan_to_dict(plan.child))
            if hasattr(plan, 'left_child') and plan.left_child:
                children.append(self._physical_plan_to_dict(plan.left_child))
            if hasattr(plan, 'right_child') and plan.right_child:
                children.append(self._physical_plan_to_dict(plan.right_child))
            
            if children:
                plan_dict['children'] = children
            
            return plan_dict
        else:
            return {'type': str(type(plan)), 'properties': {}}
    
    def log_result(self, message: str):
        """记录结果到结果文本框"""
        self.result_textbox.configure(state="normal")
        
        # 添加时间戳
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"
        
        self.result_textbox.insert("end", formatted_message)
        self.result_textbox.see("end")
        self.result_textbox.configure(state="disabled")
    
    def update_time(self):
        """更新状态栏时间"""
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.time_label.configure(text=current_time)
        self.root.after(1000, self.update_time)
    
    def run(self):
        """运行GUI应用"""
        self.root.mainloop()
        
        # 关闭时清理资源
        if self.system_manager:
            self.system_manager.shutdown()

def main():
    """主函数"""
    try:
        app = AODSQLGUI()
        app.run()
    except Exception as e:
        print(f"启动GUI失败: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
