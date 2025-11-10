@echo off
REM demo.bat - CSU数据库项目演示脚本（Windows版）
REM 用法：demo.bat [参数]

setlocal
cd /d %~dp0\..
set PYTHONPATH=%cd%

ECHO 🚀 AODSQL 数据库系统全面功能演示
ECHO ==================================

REM 1. 基础DDL操作
ECHO ===============================
ECHO 01. 基础DDL操作 - 创建表
ECHO ===============================
python -m cli.main < sql\basic\01_create_tables.sql

REM 2. 基础DML操作
ECHO ===============================
ECHO 02. 基础DML操作 - 插入数据
ECHO ===============================
python -m cli.main < sql\basic\02_insert_data.sql

REM 3. 基础查询
ECHO ===============================
ECHO 03. 基础查询 - SELECT语句
ECHO ===============================
python -m cli.main < sql\basic\03_basic_select.sql

REM 4. 语法错误测试
ECHO ===============================
ECHO 04. 语法错误测试 - 故意语法错误
ECHO ===============================
ECHO ⚠️  注意：以下测试包含故意语法错误，用于测试语法分析器
python -m cli.main < sql\basic\04_error_syntax.sql

REM 5. 语义错误测试
ECHO ===============================
ECHO 05. 语义错误测试 - 故意语义错误
ECHO ===============================
ECHO ⚠️  注意：以下测试包含故意语义错误，用于测试语义分析器
python -m cli.main < sql\basic\05_error_semantic.sql

REM 6. 高级查询功能
ECHO ===============================
ECHO 06. 高级查询 - JOIN、聚合、GROUP BY
ECHO ===============================
python -m cli.main < sql\basic\06_advanced_queries.sql

REM 7. 索引管理
ECHO ===============================
ECHO 07. 索引管理 - 创建、使用、删除索引
ECHO ===============================
python -m cli.main < sql\basic\07_index_management.sql

@REM REM 8. 视图管理
@REM ECHO ===============================
@REM ECHO 08. 视图管理 - 创建、查询、修改、删除视图
@REM ECHO ===============================
@REM python -m cli.main < sql\basic\08_view_management.sql

@REM REM 9. 触发器管理
@REM ECHO ===============================
@REM ECHO 09. 触发器管理 - 创建、显示、删除触发器
@REM ECHO ===============================
@REM python -m cli.main < sql\basic\09_trigger_management.sql

@REM REM 10. 事务管理
@REM ECHO ===============================
@REM ECHO 10. 事务管理 - BEGIN、COMMIT、ROLLBACK
@REM ECHO ===============================
@REM python -m cli.main < sql\basic\10_transaction_management.sql

@REM REM 11. 游标操作
@REM ECHO ===============================
@REM ECHO 11. 游标操作 - DECLARE、OPEN、FETCH、CLOSE
@REM ECHO ===============================
@REM python -m cli.main < sql\basic\11_cursor_operations.sql

@REM REM 12. 系统命令
@REM ECHO ===============================
@REM ECHO 12. 系统命令 - SHOW、EXPLAIN
@REM ECHO ===============================
@REM python -m cli.main < sql\basic\12_system_commands.sql

@REM REM 13. UPDATE/DELETE操作
@REM ECHO ===============================
@REM ECHO 13. 数据修改 - UPDATE、DELETE操作
@REM ECHO ===============================
@REM python -m cli.main < sql\basic\13_update_delete_operations.sql

@REM REM 14. 错误处理测试（可选）
@REM ECHO ===============================
@REM ECHO 14. 错误处理测试 - 语法和语义错误
@REM ECHO ===============================
@REM ECHO ⚠️  注意：以下测试包含故意错误，用于测试错误处理机制
@REM set /p CONTINUE_ERRTEST=是否继续执行错误处理测试？(y/N):
@REM if /I "%CONTINUE_ERRTEST%"=="y" (
@REM     python -m cli.main < sql\basic\14_error_handling.sql
@REM ) else (
@REM     ECHO 跳过错误处理测试
@REM )

ECHO ===============================
ECHO ✅ 演示完成！
ECHO ===============================
endlocal