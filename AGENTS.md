# Repository Guidelines

## Project Overview

本项目是一个针对 MySQL 数据库集群（兼容 MySQL 5.x、8.0.22+、8.0.25、8.0.32 及 GoldenDB）的运维自动化工具集。
近期仓库已完成模块化整理，主要脚本收拢于 `ymm/` 目录，并与独立的 `rdb/`、`gen_conf/`、`gen_dump/` 工具包协同工作。

核心能力涵盖：
- **GTID 运维与一致性治理**：多实例 `GTID_EXECUTED` 区间差异比对、安全重置与复制自愈。
- **拓扑探测与可视化**：双向递归拓扑探测、双主（Master-Master）循环环路拆解、终端彩色树形图与机读 JSON 双通道输出。
- **Binlog 备份与安全清理**：级联从库 `Relay_Master_Log_File` 计算、主库可写性保护、Binlog 安全清除与实时归档守护进程。
- **物理与逻辑备份迁移**：基于 Percona XtraBackup 8.0.25 的跨节点物理备份恢复编排（`rdb/`）、基于 `mysqldump` 的全自动权限/表结构/数据逻辑迁移（`gen_dump/`）。
- **HA 集群协调与配置生成**：etcd 3.x HA 状态比对与 Jinja2 配置渲染、故障降级自动强杀监控进程、级联复制配置与 DNS A 记录批量生成。

---

## Architecture & Data Flow

代码库采用去中心化、任务驱动的独立脚本架构，整体划分为四大核心子系统：

### 1. 拓扑扫描与 GTID 子系统 (`ymm/mysql_topology.py`, `ymm/compare_gtid.py`, `ymm/gtid_reset.py`, `ymm/mysql_binlog_cleaner.py`)
- **数据流向**：
  - `ymm/mysql_topology.py`：输入起始 IP，向上执行 `SHOW REPLICA STATUS`（降级 `SHOW SLAVE STATUS`）递归寻找主库；向下执行 `SHOW REPLICAS`（降级 `SHOW SLAVE HOSTS`）递归寻找副本。借助 `visited: Set[str]` 防止死循环，自动识别并破开双主互为主从的环路。输出控制台彩色树和 `----- JSON DATA BEGIN/END -----` 区间标准 JSON。
  - `ymm/compare_gtid.py`：读取两端 `@@GLOBAL.gtid_executed`，经 `parse_gtid_set` 正则切分为区间元组列表，由 `merge_intervals` 闭包合并重叠/递增区间，经 `subtract_intervals` 集合减法计算，输出源端与副本端各自独有的 GTID 集合。
  - `ymm/gtid_reset.py`：全节点一致性断言（`len(set(gtid_map.values())) == 1`）-> 记录现有复制拓扑 -> 全局执行 `SET GLOBAL read_only = 1` 锁定防写 -> 执行 `STOP SLAVE; RESET SLAVE; RESET MASTER;` -> 恢复拓扑并启动复制 -> 校验 `Slave_IO_Running` / `Slave_SQL_Running` -> 仅首节点主库解除只读（`read_only = 0`）。

### 2. 远程执行与物理编排子系统 (`ymm/myssh.py`, `rdb/myssh.py`, `ssh.py`, `rdb/xtrabackup_sync.py`, `ymm/downgrade.py`)
- **执行层双引擎**：
  - `myssh.py`（轻量引擎，首选推荐）：纯 Python 标准库，基于 OpenSSH 客户端的 `ControlMaster` 在 `/dev/shm` 共享 socket，结合 `selectors.DefaultSelector` 与 `fcntl` 非阻塞 I/O 实现毫秒级复用，命令通过 `sudo -s <<\"ssh_EOF\"` 封装。
  - `ssh.py`（元数据引擎）：基于 Paramiko，并与 CMDB 数据库联动（`myHook`），按实例名自动检索目标服务器信息。
- **物理备份恢复数据流 (`rdb/xtrabackup_sync.py`)**：
  读取 YAML 凭据 -> 步骤 0 探测 A、B 节点 SSH 免密与端口连通性 -> 停止 B 节点监控服务（`ommagentdbmoni`, `dbmoni`）-> 清理并重建 B 节点子目录（binlog, data, redo, relaylog, tmp, undo）-> A 节点执行 `xtrabackup --backup` -> 网络直传 `rsync -avzP` 至 B -> B 节点执行 `xtrabackup --prepare` -> B 节点执行 `xtrabackup --copy-back` -> 启动 B 节点服务 -> 交互式/自动清理临时备份目录。

### 3. 备份生命周期与日志清理子系统 (`ymm/binlog_backup.py`, `ymm/backup_cleaner.py`, `ymm/resync.py`, `ymm/update_backup.py`)
- **生命周期管道**：
  - `ymm/binlog_backup.py`：解析 `my.cnf` 与 `.index`，严格使用 `lines[:-1]` 切片排除当前写入文件，调用系统 `gzip -9 -c` 压缩归档至备份盘。
  - `ymm/backup_cleaner.py`：多目录扫描归档文件，保留最近两个月全量；两月以上仅保留 1/11/21 日归档。删除前执行系统 `lsof <filepath>` 确认无活动进程锁定，删除间歇休眠 2 秒平滑 I/O。
  - `ymm/resync.py`：每月 1 号自动向前/向后寻找临近备份文件重命名填补 1/11/21 日归档空洞，并通过 `rsync -av` 同步近 4 天备份至 GFS。
  - `ymm/update_backup.py`：对比 CMDB 实例清单与 etcd 键 `/db/ha/arbit/`，通过 Jinja2 模板输出备份配置增量。

### 4. 配置生成与数据迁移子系统 (`gen_conf/`, `gen_dump/`, `ymm/col_count.py`)
- `gen_conf/`：支持文本和 Excel 拓扑输入，生成级联复制配置（`rep_master_ip`）及 DNS A 记录。
- `gen_dump/migrate.py`：独立导出用户与权限为 `01_users.sql`（过滤内置系统账户），导出 DDL 为 `02_schema_<db>.sql`，数据导出使用 `--set-gtid-purged=OFF` 并写入 `03_data_<db>.sql`。
- `ymm/col_count.py`：基于 `ThreadPoolExecutor` 批量并发访问实例，查 `information_schema.tables.TABLE_ROWS` 估算值，输出大表清单 CSV。

---

## Key Directories

```
.
├── ymm/                      # 核心运维工具集（拓扑探测、GTID 比对重置、日志安全清理、备份守护进程）
│   ├── mysql_topology.py     # 拓扑递归探测与终端/JSON可视化
│   ├── gtid_reset.py         # GTID 一致性重置与主从自愈（支持 --dry-run）
│   ├── compare_gtid.py       # GTID 差异区间精确比对
│   ├── mysql_binlog_cleaner.py # 基于从库回放位的安全 Binlog 清理
│   ├── binlog_backup.py      # Binlog 实时压缩归档守护进程
│   ├── backup_cleaner.py     # 周期性备份归档生命周期清理（lsof 锁检测）
│   ├── resync.py             # GFS 归档同步与每月空洞修复
│   ├── downgrade.py          # HA 集群故障监控进程强杀降级
│   ├── col_count.py          # 多线程表规模行数估算巡检
│   ├── update_backup.py      # etcd HA 拓扑比对与 Jinja2 配置生成
│   └── myssh.py              # 纯标准库轻量 OpenSSH 客户端
├── rdb/                      # Percona XtraBackup 8.0.25 物理备份恢复编排模块
│   ├── xtrabackup_sync.py    # 主编排执行脚本（支持 --dry-run）
│   ├── myssh.py              # rdb 专用的标准库 SSH 客户端
│   ├── config.yaml           # 目标数据库连接凭据（默认端口 3309）
│   └── README.md             # 详细执行流程与操作规范
├── gen_conf/                 # 拓扑配置、DNS 记录与 Excel 数据转换工具集
│   ├── generate_config.py    # 角色级联配置生成器 (rep_master_ip)
│   ├── generate_dns.py       # DNS A 记录批量生成器
│   ├── convert_cluster_roles.py # Excel 拓扑宽表转 IDC-角色明细及 SQL
│   └── generate_config_excel.py # Excel 生成含参数复制配置
├── gen_dump/                 # MySQL 8.0.32 逻辑迁移工具包（自包含）
│   ├── migrate.py            # 导出/导入迁移脚本（支持 --dry-run 与 --dump-only）
│   ├── config.yaml           # 迁移源-目标对、数据库/用户黑白名单过滤
│   └── README.md             # 逻辑迁移操作规范
├── ssh.py                    # 根目录历史 CMDB 数据库联动的 Paramiko 远程执行模块
├── requirements.txt          # 基础依赖声明文件（PyMySQL, python-dotenv）
└── VERSION                   # 记录项目发布版本号 (v0.0.29)
```

---

## Development Commands

### 1. 环境准备与依赖安装
```bash
# 安装基础依赖
pip install -r requirements.txt

# 补齐子模块生产运行依赖（YAML、Excel、HA、日期计算及并发工具）
pip install pyyaml openpyxl paramiko etcd3 jinja2 python-dateutil requests pandas numpy

# 复制并配置环境变量
cp .env.example .env  # 填入 MYSQL_USER, MYSQL_PASSWORD, MYSQL_PORT
```

### 2. 常用运维操作命令（注意脚本位于 `ymm/` 等子目录下）
```bash
# 扫描并可视化 MySQL 复制拓扑（支持 5.x / 8.x）
python3 ymm/mysql_topology.py 192.168.1.10

# 比对两节点 GTID_EXECUTED 差异集合
python3 ymm/compare_gtid.py 192.168.1.10 192.168.1.11 --user root --password pwd

# 安全清理主库已同步的 Binlog
python3 ymm/mysql_binlog_cleaner.py 192.168.0.10

# 批量扫描超过 500 万行的大表（8 并发工作线程）
python3 ymm/col_count.py --threshold 5000000 --workers 8 --output large_tables.csv

# 生成带角色级联配置与 DNS A 记录
python3 gen_conf/generate_config.py -i gen_conf/in -o output.txt
python3 gen_conf/generate_dns.py -i source.txt --prefix ylp1- --domain local.xx.int.
```

### 3. 仿真演练与 Dry-Run 预检命令（变更前必须执行）
```bash
# GTID 重置预检（验证多节点 GTID 一致性与只读切换，不执行 RESET）
python3 ymm/gtid_reset.py --hosts 192.168.1.10,192.168.1.11 --user root --password pwd \
    --replica-user repl --replica-password replpwd --dry-run

# XtraBackup 物理备份恢复命令预览（模拟 SSH 指令与清理逻辑）
python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 --dry-run

# 数据库迁移指令预览（模拟用户权限提取与 mysqldump 参数）
python3 gen_dump/migrate.py -c gen_dump/config.yaml --dry-run
```

### 4. 代码语法验证
```bash
# 修改任何 Python 脚本后执行静态语法编译验证
python3 -m py_compile <path_to_script.py>
```

---

## Code Conventions & Common Patterns

### 1. 命名与代码风格规范
- **类名**：`PascalCase`（例如 `TopologyScanner`, `ServerRemoteExecute`, `XtrabackupSyncer`）。
- **函数与方法**：`snake_case`（例如 `parse_gtid_set`, `merge_intervals`, `is_master_writable`）。
- **内部私有方法**：`_snake_case`（例如 `_bare_run`, `_build_command`, `_load_config`）。
- **全局常量与系统集合**：`UPPER_SNAKE_CASE`（例如 `MAX_RECURSION_DEPTH`, `LOG_FILE`, `SYSTEM_DATABASES`）。
- **缩进规范**：全库推行标准 4 空格缩进（注意：历史脚本 `col_count.py` 使用了 Tab 缩进，维护时注意保持局部一致或整体格式化）。

### 2. 数据库连接与游标管理模式
- 统一使用 `pymysql`，强制指定字典游标 `DictCursor`，并显式设置 `connect_timeout`：
  ```python
  conn = pymysql.connect(
      host=host,
      port=port,
      user=user,
      password=password,
      charset="utf8mb4",
      cursorclass=pymysql.cursors.DictCursor,
      autocommit=True,       # 防止隐式事务持有元数据锁
      connect_timeout=10,
  )
  try:
      with conn.cursor() as cur:
          cur.execute(query)
          rows = cur.fetchall()
  finally:
      conn.close()
  ```
- **版本语法自适应模式**：必须优先执行 MySQL 8.0 规范语法，遇到异常平滑降级至 5.x 语法：
  ```python
  try:
      cur.execute("SHOW REPLICA STATUS")
  except Exception:
      cur.execute("SHOW SLAVE STATUS")
  ```

### 3. 远程执行模式与工具选型
- **首选 `ymm/myssh.py` 或 `rdb/myssh.py`**：仅依赖 Python 标准库，通过 OpenSSH `ControlMaster` 实现连接复用，避免磁盘 IO 堵塞（Socket 存储于 `/dev/shm`）。支持 `sudo` 命令自动注入及脚本传输原子清理。
- **慎用根目录 `ssh.py`**：`ssh.py` 在模块加载顶层实例化 `myhook = myHook()`，导入时即触发 CMDB 数据库连接。若无需 CMDB 实例解析，应直接引入 `myssh.py`。

### 4. 异常处理与退出码规范
- `0`：操作成功或比对结果完全一致。
- `1`：逻辑校验失败、GTID 分歧、业务中止。
- `2`：网络连接中断、认证失败或参数非法。
- `130`：捕获 `KeyboardInterrupt` 正常中断。
- **Fail-Fast 原则**：进行破坏性操作前必须前置断言。例如 `ymm/gtid_reset.py` 必须验证全部实例 `gtid_executed` 严格相同方可继续。

### 5. 并发与后台模式
- **批量数据探针**：使用 `concurrent.futures.ThreadPoolExecutor` 配合 `as_completed`，禁止使用全表 `COUNT(*)`，统一使用 `information_schema.tables.TABLE_ROWS` 估算。
- **守护循环**：`binlog_backup.py` 等后台进程必须采用安全 `while True:` 循环配合 `time.sleep()`，并在循环体内捕获局部异常防止守护进程异常崩溃。

### 6. 生产安全红线与防护模式
- **只读前置**：在执行拓扑解绑、GTID 清零前，必须全局置为只读（`SET GLOBAL read_only = 1`），防止业务写入造成脑裂；完成后仅放开主库。
- **回滚与保护**：清理 Binlog 必须检查主库是否可写（`read_only == 0`），并取从库最小序列号前置减 1（`decrement_binlog_file`），确保未同步日志不被误删。
- **活跃文件保护**：Binlog 备份脚本解析 `.index` 时必须排除最后一行活跃文件（`lines[:-1]`）；清理备份前调用 `lsof` 校验文件占用状态。
- **防环设计**：递归遍历拓扑必须使用 `visited` 集合并硬编码 `MAX_RECURSION_DEPTH = 4`。
- **临时隔离**：临时目录使用时间戳隔离（如 `backup_tmp_<YYYYMMDD_HHMMSS>`），SSH socket 放置于 `/dev/shm`，重启自动销毁。

---

## Important Files

### 核心入口与运维脚本（位于 `ymm/` 及子模块）
- `ymm/mysql_topology.py`：拓扑扫描主入口，支持终端高亮及结构化 JSON 输出。
- `ymm/gtid_reset.py`：生产级 GTID 一致性重置及主从拓扑重建工具，含完整 `--dry-run`。
- `ymm/compare_gtid.py`：纯算法级 GTID 区间拆分、合并与差集计算工具。
- `ymm/mysql_binlog_cleaner.py`：递归扫描级联从库并安全清理主库 Binlog。
- `rdb/xtrabackup_sync.py`：跨主机物理全备、传输、准备与拷贝恢复主控脚本。
- `gen_dump/migrate.py`：生产级用户权限、DDL 及数据逻辑迁移工具。
- `ymm/binlog_backup.py`：Binlog 实时压缩归档守护进程。
- `ymm/backup_cleaner.py`：基于日期策略与 `lsof` 锁校验的过期备份清理脚本。
- `ymm/downgrade.py`：基于 etcd 状态与健康检测的故障节点监控进程强杀降级脚本。
- `ymm/col_count.py`：多线程大表行数评估采集工具。
- `gen_conf/generate_config.py`：集群主从级联复制配置生成工具。

### 配置文件与环境模板
- `.env`：本地 MySQL 默认连接凭据（`MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_PORT`，受 `.gitignore` 保护）。
- `rdb/config.yaml`：物理备份目标 GoldenDB 实例认证配置。
- `gen_dump/config.yaml`：逻辑迁移源-目标对、数据库/用户黑白名单过滤规则。
- `requirements.txt`：项目基础依赖定义（`PyMySQL`, `python-dotenv`）。
- `VERSION`：记录项目发布版本号（当前为 `v0.0.29`）。

### 核心执行与底层库
- `ymm/myssh.py` / `rdb/myssh.py`：零外部依赖的高性能 OpenSSH 连接复用器。
- `ssh.py`：基于 Paramiko 与 CMDB 数据库联动的远程执行器（位于根目录）。

---

## Runtime/Tooling Preferences

- **Python 运行时**：建议使用 **Python 3.10+**（基础运维脚本支持 Python 3.7+；若包含现代类型注解语法需 3.10+）。
- **包管理器**：标准 `pip`。未配置 `poetry`、`pipenv`、`conda` 等工具。
- **外部操作系统二进制与守护进程依赖**：
  - `ssh` / `scp`：用于 `myssh.py` 建立 ControlMaster socket（位于 `/dev/shm`）。
  - `xtrabackup`：Percona XtraBackup 8.0.25（远程主机执行物理热备）。
  - `rsync`：用于大文件跨机同步。
  - `mysqldump` / `mysql` / `mysqladmin`：用于逻辑导出、导入及存活探测。
  - `gzip`：用于 Binlog 归档压缩。
  - `lsof`：用于文件句柄占用检测。
  - `etcd` (v3 API)：监听在 `127.0.0.1:2379`，用于 HA 状态协同。
- **配置关键字细节注意**：
  - `gen_conf/generate_config.py` 输出格式为 `rep_master_ip=<ip>`，与历史文档中部分记载的 `repl_master_ip` 存在拼写差异，对接下游工具时须注意。

---

## Testing & QA

### 1. 测试现状
- **无自动化测试框架**：代码库未集成 `pytest`、`unittest`、`tox` 等测试套件，亦无 `tests/` 目录。
- **注意辨析**：历史原型脚本（如 `t.py`）**并非测试脚本**，而是轻量 Web 服务或调试原型。
- **历史测试遗留**：`backup_cleaner.py` 默认包含 `/tmp/backup_test` 测试目录；`example_hostname.py` 和 `myssh.py:__main__` 包含针对 `192.168.0.10` 的单点连通性测试代码。

### 2. 验证机制与防线
代码库的质量保障依赖三层内置防线：
1. **CLI `--dry-run` 仿真拦截**：`ymm/gtid_reset.py`、`rdb/xtrabackup_sync.py`、`gen_dump/migrate.py` 均内置了模拟运行参数，可打印预期执行的 SQL、系统命令并在离线状态下构造模拟返回。
2. **前置安全断言**：运行期强制校验主库可写性（`is_master_writable`）、多实例 GTID 完全一致性、`lsof` 锁状态以及递归防环（深度 <= 4）。
3. **连通性前置嗅探**：`rdb/xtrabackup_sync.py` 在步骤 0 自动探测 A、B 双节点的 SSH 免密与端口连通性。

### 3. AI 助手开发与代码修改质量验证准则
在此仓库新增功能或修复 Bug 时，必须严格执行以下 5 步验证：

1. **静态语法检查（强制）**：
   ```bash
   python3 -m py_compile <path_to_modified_file.py>
   ```
2. **纯算法逻辑隔离验证**：
   对于纯解析或算法函数（如 `ymm/compare_gtid.py` 的区间运算、`gen_conf/` 的文本解析、`ymm/backup_cleaner.py` 的保留日期正则匹配），编写独立的 Python 标准库 `unittest` 脚本，在内存中覆盖边界场景（如空输入、连续区间、孤立区间等）。
3. **CLI 命令行解析验证**：
   所有支持命令行参数的脚本，修改后必须执行 `--help`，确保 `argparse` 参数声明无冲突、类型定义正确：
   ```bash
   python3 <script.py> --help
   ```
4. **网络/数据库隔离 Mock**：
   在无真实生产 MySQL 或 SSH 环境下验证逻辑时，使用 `unittest.mock`（`patch` / `MagicMock`）模拟 `pymysql.connect` 返回字典游标及执行结果，严禁向脚本中的固定测试 IP 发起破坏性网络请求。
5. **绝对遵守生产安全红线**：
   - 绝不可绕过或删除现有的 `read_only` 保护锁、`lsof` 检查及 `--dry-run` 逻辑。
   - 调试含有破坏性操作（`RESET MASTER`, `kill -9`, `rm -rf`, `PURGE BINARY LOGS`）的代码时，必须优先使用 `--dry-run` 观察打印日志。
