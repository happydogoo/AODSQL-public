# SQL测试文件分类说明

本目录包含了AODSQL数据库系统的全面功能测试SQL文件，按功能分类组织。

## 文件分类

### 基础功能测试
- **01_create_tables.sql** - 基础DDL操作，创建和删除表
- **02_insert_data.sql** - 基础DML操作，插入数据
- **03_basic_select.sql** - 基础查询，SELECT语句
- **04_error_syntax.sql** - 语法错误测试
- **05_error_semantic.sql** - 语义错误测试

### 高级功能测试
- **06_advanced_queries.sql** - 高级查询功能
  - JOIN操作（INNER, LEFT, RIGHT, FULL）
  - 聚合函数（COUNT, AVG, MAX, MIN）
  - GROUP BY分组查询
  - ORDER BY排序查询
  - 复杂查询组合

- **07_index_management.sql** - 索引管理
  - 创建索引
  - 显示索引信息
  - 使用索引的查询优化
  - 删除索引

- **08_view_management.sql** - 视图管理
  - 创建简单视图和聚合视图
  - 查询视图
  - 修改视图定义
  - 删除视图

- **09_trigger_management.sql** - 触发器管理
  - 创建BEFORE/AFTER触发器
  - 触发器条件检查
  - 显示触发器信息
  - 删除触发器

- **10_transaction_management.sql** - 事务管理
  - 开始事务（BEGIN TRANSACTION）
  - 提交事务（COMMIT）
  - 回滚事务（ROLLBACK）
  - 事务中的数据操作

- **11_cursor_operations.sql** - 游标操作
  - 声明游标（DECLARE CURSOR）
  - 打开游标（OPEN）
  - 获取数据（FETCH）
  - 关闭游标（CLOSE）

- **12_system_commands.sql** - 系统命令
  - 显示表（SHOW TABLES）
  - 显示列（SHOW COLUMNS）
  - 显示索引（SHOW INDEX）
  - 显示触发器（SHOW TRIGGERS）
  - 显示视图（SHOW VIEWS）
  - 解释查询计划（EXPLAIN）

- **13_update_delete_operations.sql** - 数据修改操作
  - UPDATE更新操作
  - DELETE删除操作
  - 条件更新和删除
  - 批量操作

- **14_error_handling.sql** - 错误处理测试
  - 语法错误测试
  - 语义错误测试
  - 约束违反测试
  - 数据类型错误测试

## 使用方法

### 完整功能演示
```bash
bash scripts/demo.sh
```

### 核心功能演示
```bash
bash scripts/demo_simple.sh
```

### 单独测试某个功能
```bash
python3 -m cli.main < sql/basic/06_advanced_queries.sql
```

## 功能覆盖

本测试套件覆盖了AODSQL数据库系统的以下核心功能：

### DDL (数据定义语言)
- ✅ CREATE TABLE - 创建表
- ✅ DROP TABLE - 删除表
- ✅ CREATE INDEX - 创建索引
- ✅ DROP INDEX - 删除索引
- ✅ CREATE VIEW - 创建视图
- ✅ DROP VIEW - 删除视图
- ✅ ALTER VIEW - 修改视图
- ✅ CREATE TRIGGER - 创建触发器
- ✅ DROP TRIGGER - 删除触发器

### DML (数据操作语言)
- ✅ INSERT - 插入数据
- ✅ UPDATE - 更新数据
- ✅ DELETE - 删除数据
- ✅ SELECT - 查询数据

### 查询功能
- ✅ 基础查询（SELECT, FROM, WHERE）
- ✅ 连接查询（INNER, LEFT, RIGHT, FULL JOIN）
- ✅ 聚合函数（COUNT, SUM, AVG, MIN, MAX）
- ✅ 分组查询（GROUP BY）
- ✅ 排序查询（ORDER BY）
- ✅ 子查询支持

### 高级功能
- ✅ 事务管理（BEGIN, COMMIT, ROLLBACK）
- ✅ 游标操作（DECLARE, OPEN, FETCH, CLOSE）
- ✅ 触发器（BEFORE/AFTER, 条件检查）
- ✅ 视图管理（创建、修改、删除）
- ✅ 索引管理（创建、使用、删除）
- ✅ 系统命令（SHOW, EXPLAIN）

### 错误处理
- ✅ 语法错误检测
- ✅ 语义错误检测
- ✅ 约束违反检测
- ✅ 数据类型错误检测

## 注意事项

1. **执行顺序**：建议按照文件编号顺序执行，因为某些测试依赖于前面的数据
2. **错误测试**：14_error_handling.sql包含故意错误，用于测试错误处理机制
3. **交互式测试**：完整演示脚本包含交互式选择，可以选择是否执行错误处理测试
4. **环境要求**：确保Python环境和依赖已正确安装
