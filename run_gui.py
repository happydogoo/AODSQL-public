#!/usr/bin/env python3
"""
AODSQL GUI 启动脚本
"""

import sys
import os

def main():
    """启动GUI"""
    try:
        # 检查依赖
        try:
            import customtkinter
            import rich
        except ImportError as e:
            print(f"❌ 缺少依赖: {e}")
            print("请运行: pip3 install customtkinter rich")
            return
        
        # 启动GUI
        print("🚀 启动 AODSQL GUI...")
        os.system(f"{sys.executable} gui.py")
        
    except Exception as e:
        print(f"❌ 启动失败: {e}")

if __name__ == "__main__":
    main()

