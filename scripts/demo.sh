#!/bin/bash
# demo.sh - CSU数据库项目演示脚本（Mac版）
# 用法：bash demo.sh [参数]

# 切换到脚本所在目录的父目录（项目根目录）
cd "$(dirname "$0")/.."

# 设置PYTHONPATH为项目根目录
export PYTHONPATH="$(pwd)"

echo "🚀 AODSQL 数据库系统全面功能演示"
echo "=================================="

# 1. 基础DDL操作
echo "==============================="
echo "01. 基础DDL操作 - 创建表"
echo "==============================="
python3 -m cli.main < sql/basic/01_create_tables.sql

# 2. 基础DML操作
echo "==============================="
echo "02. 基础DML操作 - 插入数据"
echo "==============================="
python3 -m cli.main < sql/basic/02_insert_data.sql

# 3. 基础查询
echo "==============================="
echo "03. 基础查询 - SELECT语句"
echo "==============================="
python3 -m cli.main < sql/basic/03_basic_select.sql

# 4. 语法错误测试
echo "==============================="
echo "04. 语法错误测试 - 故意语法错误"
echo "==============================="
echo "⚠️  注意：以下测试包含故意语法错误，用于测试语法分析器"
python3 -m cli.main < sql/basic/04_error_syntax.sql

# 5. 语义错误测试
echo "==============================="
echo "05. 语义错误测试 - 故意语义错误"
echo "==============================="
echo "⚠️  注意：以下测试包含故意语义错误，用于测试语义分析器"
python3 -m cli.main < sql/basic/05_error_semantic.sql

# 6. 高级查询功能
echo "==============================="
echo "06. 高级查询 - JOIN、聚合、GROUP BY"
echo "==============================="
python3 -m cli.main < sql/basic/06_advanced_queries.sql

# 7. 索引管理
echo "==============================="
echo "07. 索引管理 - 创建、使用、删除索引"
echo "==============================="
python3 -m cli.main < sql/basic/07_index_management.sql

# 8. 视图管理
echo "==============================="
echo "08. 视图管理 - 创建、查询、修改、删除视图"
echo "==============================="
python3 -m cli.main < sql/basic/08_view_management.sql

# 9. 触发器管理
echo "==============================="
echo "09. 触发器管理 - 创建、显示、删除触发器"
echo "==============================="
python3 -m cli.main < sql/basic/09_trigger_management.sql

# 10. 事务管理
echo "==============================="
echo "10. 事务管理 - BEGIN、COMMIT、ROLLBACK"
echo "==============================="
python3 -m cli.main < sql/basic/10_transaction_management.sql

# 11. 游标操作
echo "==============================="
echo "11. 游标操作 - DECLARE、OPEN、FETCH、CLOSE"
echo "==============================="
python3 -m cli.main < sql/basic/11_cursor_operations.sql

# 12. 系统命令
echo "==============================="
echo "12. 系统命令 - SHOW、EXPLAIN"
echo "==============================="
python3 -m cli.main < sql/basic/12_system_commands.sql

# 13. UPDATE/DELETE操作
echo "==============================="
echo "13. 数据修改 - UPDATE、DELETE操作"
echo "==============================="
python3 -m cli.main < sql/basic/13_update_delete_operations.sql

# 14. 错误处理测试（可选）
echo "==============================="
echo "14. 错误处理测试 - 语法和语义错误"
echo "==============================="
echo "⚠️  注意：以下测试包含故意错误，用于测试错误处理机制"
read -p "是否继续执行错误处理测试？(y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    python3 -m cli.main < sql/basic/14_error_handling.sql
else
    echo "跳过错误处理测试"
fi

echo "==============================="
echo "✅ 演示完成！"
echo "===============================" 