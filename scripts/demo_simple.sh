#!/bin/bash
# demo_simple.sh - AODSQL数据库系统简化演示脚本（Mac版）
# 用法：bash demo_simple.sh

# 设置PYTHONPATH为项目根目录
export PYTHONPATH="$(pwd)"

echo "🚀 AODSQL 数据库系统核心功能演示"
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

# 4. 高级查询功能
echo "==============================="
echo "04. 高级查询 - JOIN、聚合、GROUP BY"
echo "==============================="
python3 -m cli.main < sql/basic/06_advanced_queries.sql

# 5. 系统命令
echo "==============================="
echo "05. 系统命令 - SHOW、EXPLAIN"
echo "==============================="
python3 -m cli.main < sql/basic/12_system_commands.sql

echo "==============================="
echo "✅ 核心功能演示完成！"
echo "==============================="
echo ""
echo "💡 提示：运行 'bash demo.sh' 查看完整功能演示"
