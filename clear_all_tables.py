# -*- coding: utf-8 -*-
"""
清除所有表的信息和结构
包括表数据、索引、视图等所有数据库对象
"""
import os
import sys
import glob

# 添加项目路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.engine.catalog_manager import CatalogManager


def clear_all_database_objects():
    """清除所有数据库对象"""
    print("🧹 开始清除所有数据库对象...")
    
    try:
        # 1. 初始化组件
        catalog_manager = CatalogManager()
        
        # 2. 清除所有视图
        print("\n📋 清除所有视图...")
        views = catalog_manager.list_views()
        for view_name in views:
            try:
                catalog_manager.delete_view(view_name)
                print(f"  ✅ 已删除视图: {view_name}")
            except Exception as e:
                print(f"  ❌ 删除视图 {view_name} 失败: {str(e)}")
        
        # 3. 清除所有表
        print("\n📊 清除所有表...")
        tables = catalog_manager.list_tables()
        for table_name in tables:
            try:
                # 删除表文件
                table_info = catalog_manager.get_table(table_name)
                if table_info.file_name and os.path.exists(table_info.file_name):
                    os.remove(table_info.file_name)
                    print(f"  ✅ 已删除表文件: {table_info.file_name}")
                
                # 删除索引文件
                for index_name, index_info in table_info.indexes.items():
                    if index_info.file_name and os.path.exists(index_info.file_name):
                        os.remove(index_info.file_name)
                        print(f"  ✅ 已删除索引文件: {index_info.file_name}")
                
                # 从目录中删除表
                catalog_manager.delete_table(table_name)
                print(f"  ✅ 已从目录中删除表: {table_name}")
                
            except Exception as e:
                print(f"  ❌ 删除表 {table_name} 失败: {str(e)}")
        
        # 4. 清除目录文件
        print("\n🗂️ 清除目录文件...")
        catalog_file = 'catalog.json'
        if os.path.exists(catalog_file):
            # 备份原文件
            backup_file = f"{catalog_file}.backup"
            if os.path.exists(backup_file):
                os.remove(backup_file)
            os.rename(catalog_file, backup_file)
            print(f"  ✅ 已备份目录文件到: {backup_file}")
        
        # 重新创建空的目录文件
        catalog_manager._save_catalog()
        print(f"  ✅ 已创建新的空目录文件")
        
        # 5. 清除其他数据库文件
        print("\n🗑️ 清除其他数据库文件...")
        db_files = glob.glob("*.db")
        for db_file in db_files:
            try:
                os.remove(db_file)
                print(f"  ✅ 已删除数据库文件: {db_file}")
            except Exception as e:
                print(f"  ❌ 删除文件 {db_file} 失败: {str(e)}")
        
        # 6. 清除索引文件
        idx_files = glob.glob("*.idx")
        for idx_file in idx_files:
            try:
                os.remove(idx_file)
                print(f"  ✅ 已删除索引文件: {idx_file}")
            except Exception as e:
                print(f"  ❌ 删除文件 {idx_file} 失败: {str(e)}")
        
        # 7. 清除日志文件
        log_files = glob.glob("*.log")
        for log_file in log_files:
            try:
                os.remove(log_file)
                print(f"  ✅ 已删除日志文件: {log_file}")
            except Exception as e:
                print(f"  ❌ 删除文件 {log_file} 失败: {str(e)}")
        
        # 8. 清除数据目录中的文件
        data_dir = "data"
        if os.path.exists(data_dir):
            print(f"\n📁 清除数据目录: {data_dir}")
            for file_path in glob.glob(os.path.join(data_dir, "*")):
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                        print(f"  ✅ 已删除文件: {file_path}")
                    elif os.path.isdir(file_path):
                        import shutil
                        shutil.rmtree(file_path)
                        print(f"  ✅ 已删除目录: {file_path}")
                except Exception as e:
                    print(f"  ❌ 删除 {file_path} 失败: {str(e)}")
        
        print("\n🎉 所有数据库对象清除完成！")
        
        # 9. 显示清理结果
        print("\n📊 清理结果:")
        print(f"  - 已删除表数量: {len(tables)}")
        print(f"  - 已删除视图数量: {len(views)}")
        print(f"  - 已删除数据库文件数量: {len(db_files)}")
        print(f"  - 已删除索引文件数量: {len(idx_files)}")
        print(f"  - 已删除日志文件数量: {len(log_files)}")
        
        # 10. 验证清理结果
        print("\n🔍 验证清理结果:")
        new_catalog_manager = CatalogManager()
        remaining_tables = new_catalog_manager.list_tables()
        remaining_views = new_catalog_manager.list_views()
        
        if not remaining_tables and not remaining_views:
            print("  ✅ 所有表和视图已成功清除")
        else:
            print(f"  ⚠️ 仍有残留对象: 表({len(remaining_tables)}), 视图({len(remaining_views)})")
        
    except Exception as e:
        print(f"\n❌ 清除过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()


def clear_with_confirmation():
    """带确认的清除操作"""
    print("⚠️  警告: 此操作将清除所有数据库对象!")
    print("包括:")
    print("  - 所有表的数据和结构")
    print("  - 所有索引")
    print("  - 所有视图")
    print("  - 所有数据库文件")
    print("  - 所有日志文件")
    print()
    
    # 检查当前状态
    catalog_manager = CatalogManager()
    tables = catalog_manager.list_tables()
    views = catalog_manager.list_views()
    
    print(f"当前数据库状态:")
    print(f"  - 表数量: {len(tables)}")
    print(f"  - 视图数量: {len(views)}")
    
    if tables:
        print(f"  - 表列表: {', '.join(tables)}")
    if views:
        print(f"  - 视图列表: {', '.join(views)}")
    
    print()
    confirm = input("确认要清除所有数据库对象吗? (输入 'YES' 确认): ")
    
    if confirm == 'YES':
        clear_all_database_objects()
    else:
        print("❌ 操作已取消")


def main():
    """主函数"""
    print("🧹 AODSQL 数据库清理工具")
    print("=" * 50)
    
    # 检查是否在正确的目录
    if not os.path.exists("src/engine"):
        print("❌ 错误: 请在项目根目录下运行此脚本")
        return
    
    clear_with_confirmation()


if __name__ == "__main__":
    main()
