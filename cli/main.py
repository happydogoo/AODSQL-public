# main.py (REVISED and CORRECT)

import sys
import os

# 确保所有模块都能被找到
# 添加项目根目录到 Python 路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cli.system_manager import SystemManager
from cli.cli_interface import CLIInterface

def main():
    """主函数，启动数据库的交互式命令行。"""
    system_manager = None
    try:
        # 1. 初始化系统总管理器。它现在是唯一的顶层对象。
        system_manager = SystemManager(base_data_dir='data')
        
        # 2. (可选) 初始化一个默认数据库，方便首次使用
        # 这些方法在你的SystemManager中已经存在，可以直接使用
        if not os.path.exists('data/default'):
             system_manager.create_database('default')
        system_manager.use_database('default')

        # 3. 创建 CLI 接口，只向它传递总管理器
        cli = CLIInterface(system_manager=system_manager)
        
        # 4. 检查是否从文件重定向输入
        if not sys.stdin.isatty():
            # 从文件重定向输入，处理多行SQL
            file_content = sys.stdin.read()
            sql_statements = cli.read_multiline_sql_from_file(file_content)
            
            for sql_statement in sql_statements:
                if sql_statement.strip():
                    cli.process_sql_input(sql_statement)
        else:
            # 交互式模式，运行 CLI 主循环
            cli.run()
        
    except Exception as e:
        print(f"❌ 系统启动或运行时发生致命错误: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保系统在退出时能正确关闭
        if system_manager:
            system_manager.shutdown()

if __name__ == "__main__":
    main()