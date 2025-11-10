好的，以下是根据你项目的实际结构和功能，**完善后的 `README.md`**（中文，结构清晰，突出核心模块、开发规范、运行与测试说明、性能测试方法等，适合团队协作和新成员快速上手）：

---

# AOD数据库

AOD数据库是一个教学型的关系型数据库系统，支持SQL解析、查询优化、事务、B+树索引、触发器、视图等核心功能，采用模块化设计，便于扩展和维护。

## 目录结构

```
.
├── cli/                    # 命令行接口与主控模块
│   ├── main.py             # CLI 启动入口
│   ├── system_manager.py   # 系统管理器
│   ├── cli_interface.py    # 交互式命令行实现
│   ├── plan_converter.py   # 逻辑计划与物理计划转换
│   └── ...
├── engine/                 # 数据库核心引擎
│   ├── operator.py         # 物理算子实现
│   ├── catalog_manager.py  # 系统目录管理
│   ├── real_storage_engine.py # 真实存储引擎
│   ├── btree_manager.py    # B+树索引管理
│   ├── trigger_manager.py  # 触发器管理
│   ├── view_manager.py     # 视图管理
│   ├── executor.py         # 执行器
│   ├── transaction/        # 事务与日志子模块
│   └── ...
├── storage/                # 存储层实现
│   ├── buffer.py           # 缓冲池管理
│   ├── btreepage.py        # B+树页结构与序列化
│   └── ...
├── sql/                    # SQL 脚本样例
│   ├── basic/              # 基础功能SQL
│   └── extend/             # 性能与扩展测试SQL
├── tests/                  # 测试用例
│   ├── half/
│   │   └── test_backend.py # 综合后端测试
│   ├── bptree/             # B+树相关测试
│   ├── optimizer_index/    # 优化器与索引相关测试
│   └── ...
├── scripts/                # 辅助脚本
│   └── gen_large_table.py  # 生成大表数据的脚本
├── demo.bat                # Windows 一键演示脚本
├── README.md               # 项目说明文档
└── ...
```

## 主要功能模块

- **SQL编译与执行**：支持SQL的词法、语法、语义分析，生成逻辑计划、物理计划并执行。
- **存储引擎**：支持磁盘存储、缓冲池管理、表空间与索引空间管理。
- **B+树索引**：高效的索引结构，支持唯一性约束、分裂、合并、再平衡等操作。
- **事务与日志**：支持基本的事务管理（begin/commit/rollback）和日志恢复。
- **查询优化**：支持规则/代价驱动的优化、谓词下推、索引选择等。
- **视图与触发器**：支持视图定义、触发器注册与执行。
- **性能测试**：内置大表生成与索引性能对比脚本。

## 开发与编码规范

- 遵循 [PEP8](https://peps.python.org/pep-0008/) 及团队自定义规范（见项目内文档/注释）。
- 变量/函数/类/常量命名分别采用 snake_case、PascalCase、UPPER_SNAKE_CASE。
- 所有公共函数、类、方法必须有 docstring，推荐类型提示。
- 错误与日志输出统一使用 [loguru](https://github.com/Delgan/loguru)。
- 详细规范见 `storage/btreepage.py` 文件头注释。

## 快速开始

1. **环境准备**
   - 推荐 Python 3.8+，建议使用虚拟环境（如 `.venv/`）。
   - 安装依赖（如有 `requirements.txt`）：
     ```bash
     pip install -r requirements.txt
     ```

2. **运行数据库系统**
   - Windows 下可直接运行：
     ```bash
     demo.bat
     ```
   - 或手动启动 CLI：
     ```bash
     cd cli
     python main.py
     ```

3. **加载/执行SQL脚本**
   - 在 CLI 中输入：
     ```
     .read sql/basic/01_create_tables.sql
     .read sql/basic/02_insert_data.sql
     .read sql/basic/03_basic_select.sql
     ```

4. **性能测试**
   - 生成大表数据：
     ```bash
     python scripts/gen_large_table.py
     ```
   - 在 CLI 中执行：
     ```
     .read sql/extend/00_gen_large_table.sql
     .read sql/extend/20_perf_test_no_index.sql
     .read sql/extend/21_create_index.sql
     ```

## 测试

- 单元测试位于 `tests/` 目录，推荐使用 `pytest` 运行：
  ```bash
  pytest
  ```

## 贡献指南

- 请严格遵守分支、提交、代码风格规范（详见团队规范）。
- 新功能请在 `feature/xxx` 分支开发，提交前自测并补充测试用例。
- 重要变更请提交 PR 并请求 Code Review。

## 致谢

本项目为教学/实验用途，部分设计参考了 MySQL 等开源数据库实现。

---

如有问题或建议，欢迎在 Issue 区留言或联系项目维护者。