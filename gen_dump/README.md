# MySQL 8.0.32 数据库迁移工具 (gen_dump)

本目录提供了一个自包含、不依赖上层目录任何代码的 MySQL 8.0.32 数据库迁移与备份工具（`migrate.py`）。

---

## 核心特性

1. **迁移用户与权限 (独立导出)**：
   - 提取源库非系统内置用户的 `SHOW CREATE USER` 与 `SHOW GRANTS` 语句。
   - 导出为单独的 `01_users.sql` 文件，**绝不与数据库结构或数据混杂**。

2. **迁移数据库表结构**：
   - 使用 `mysqldump` 导出数据库表结构、视图与触发器，生成 `02_schema_<dbname>.sql`。

3. **可选数据迁移**：
   - **默认不迁移数据**（控制迁移风险与执行耗时）。
   - 支持在配置文件 `migrations` 的特定映射项中指定 `include_data: true`，或在命令行使用 `--include-data` 全局开启数据导出（生成 `03_data_<dbname>.sql`）。

4. **GTID 安全控制**：
   - 使用 `mysqldump` 导出时显式配置 `--set-gtid-purged=OFF`，避免在导入时变更目标库的 `gtid_purged` 变量。

5. **灵活的黑白名单过滤**：
   - 配置文件提供 `ignore_databases` 与 `ignore_users`（黑名单），自动跳过系统库与指定账号。
   - 配置文件提供 `include_databases` 与 `include_users`（白名单）。
   - 程序自动过滤 MySQL 系统内置数据库（`information_schema`, `performance_schema`, `mysql`, `sys`）与系统内置账号（`root@localhost`, `mysql.sys` 等）。

6. **多源 - 多目标批量映射 (凭据继承)**：
   - 支持配置多组 `source` -> `target` 主机映射对。
   - `common` 节点提供通用端口、用户名和密码，各映射项继承或单独覆盖。

7. **零外部代码依赖**：
   - 独立在 `gen_dump/` 目录运行，不包含或调用上层目录代码。

---

## 文件结构

```
gen_dump/
├── config.yaml    # 配置文件模板 (定义通用凭据、映射对与黑白名单)
├── migrate.py     # 迁移工具主入口脚本
└── README.md      # 工具使用说明文档
```

运行导出的 SQL 文件保存于指定的 `output_dir` (默认 `./dumps/`) 下，按源主机 IP 隔离存储：
```
dumps/
└── 192.168.1.10/
    ├── 01_users.sql           # 独立的用户与权限 DDL
    ├── 02_schema_app_db.sql    # app_db 表结构
    └── 03_data_app_db.sql      # app_db 数据 (仅在 include_data: true 时生成)
```

---

## 配置文件说明 (`config.yaml`)

```yaml
# 源库与目标库共享的通用凭据
common:
  port: 3306
  user: "root"
  password: "your_shared_password"

# 多源 -> 多目标 迁移映射对列表
migrations:
  - source: "192.168.1.10"
    target: "192.168.2.10"
    include_data: false   # 本组仅迁移表结构 + 用户

  - source: "192.168.1.11"
    target: "192.168.2.11"
    include_data: true    # 本组迁移表结构 + 数据 + 用户

# 过滤规则配置
filter:
  ignore_databases:
    - "test"
    - "tmp_db"
  include_databases: []

  ignore_users:
    - "test_user"
    - "backup_user"
  include_users: []

output_dir: "./dumps"
```

---

## 快速上手与命令示例

### 1. 安装依赖
工具仅依赖 Python 3 及标准第三方库 `PyMySQL` 和 `PyYAML`：
```bash
pip install pymysql pyyaml
```

### 2. 试运行模式 (Dry-Run)
预览将要提取的用户、数据库清单以及 mysqldump 命令：
```bash
python3 gen_dump/migrate.py -c gen_dump/config.yaml --dry-run
```

### 3. 执行默认迁移 (导出 + 在线导入目标库)
```bash
python3 gen_dump/migrate.py -c gen_dump/config.yaml
```

### 4. 仅导出 SQL 文件到本地，不导入目标库 (`--dump-only`)
```bash
python3 gen_dump/migrate.py -c gen_dump/config.yaml --dump-only -o ./my_dumps
```

### 5. 命令行强制全局开启数据迁移
```bash
python3 gen_dump/migrate.py -c gen_dump/config.yaml --include-data
```

### 6. 精确筛选单组 IP 映射对进行迁移
```bash
python3 gen_dump/migrate.py -c gen_dump/config.yaml --source 192.168.1.10
```

---

## CLI 参数汇总

| 参数 | 缩写 | 说明 |
| :--- | :--- | :--- |
| `--config` | `-c` | 配置文件路径 (默认: `gen_dump/config.yaml`) |
| `--include-data` | | 强制全局开启数据迁移 (覆盖配置文件的 `include_data`) |
| `--dump-only` | | 仅导出 SQL 文件至输出目录，不连接目标库导入 |
| `--dry-run` | | 试运行模式，预览命令不执行实际导出与导入 |
| `--output-dir` | `-o` | 指定导出 SQL 文件的根目录 (覆盖配置文件的 `output_dir`) |
| `--source` | | 筛选仅执行指定源主机 IP 的迁移任务 |
| `--target` | | 筛选仅执行指定目标主机 IP 的迁移任务 |
