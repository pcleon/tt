# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

这是一个 MySQL 运维工具集，提供数据库运维相关的自动化脚本和工具。主要功能包括 GTID 管理、拓扑扫描、binlog 管理、备份清理等。

## 环境配置

### 依赖安装
```bash
pip install -r requirements.txt
```

核心依赖：
- `PyMySQL>=1.0.2` - MySQL 数据库连接
- `python-dotenv>=1.0.0` - 环境变量管理
- `etcd3` - etcd 客户端（部分脚本需要）
- `paramiko` - SSH 远程执行（ssh.py）
- `jinja2` - 模板渲染（update_backup.py）

### 环境变量
所有脚本从 `.env` 文件或环境变量读取 MySQL 凭据：
```bash
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_PORT=3306
```

部分脚本支持其他配置：
- `SOURCE_HOST`、`SOURCE_DB`、`IP_QUERY`（col_count.py）
- `ssh_user`（ssh.py）

## 核心模块

### GTID 管理工具

**gtid_reset.py** - GTID 一致性检查和重置
```bash
python3 gtid_reset.py --hosts 192.168.1.10,192.168.1.11 --user root --password 123456 \
    --replica-user repl --replica-password replpass --dry-run
```
功能：
- 检查多个实例的 GTID_EXECUTED 是否一致
- 支持安全重置（先设置只读）
- 自动恢复原有复制拓扑

**compare_gtid.py** - 比较两个 MySQL 实例的 GTID 差异
```bash
python3 compare_gtid.py 192.168.0.10 192.168.0.11 --user root --password pwd
```
功能：
- 解析和比较 GTID 集合
- 输出 source 和 replica 各自独有的 GTID 区间

### MySQL 拓扑管理

**mysql_topology.py** - MySQL 拓扑扫描和可视化
```bash
python3 mysql_topology.py 192.168.1.10
```
功能：
- 递归探测主从关系（向上找 Master，向下找 Slaves）
- 支持双主架构检测
- 输出终端可视化拓扑图和 JSON 格式数据（便于程序解析）
- 支持 MySQL 5.x 和 8.x 语法

### Binlog 管理工具

**mysql_binlog_cleaner.py** - 自动清理 MySQL 二进制日志
```bash
python3 mysql_binlog_cleaner.py 192.168.0.10
```
功能：
- 递归查找主库和所有级联从库
- 安全清理 binlog（基于所有从库的 Relay_Master_Log_File）
- 支持最大递归深度 4 层
- 日志记录到 `/tmp/mysql_binlog_cleaner.log`

**binlog_backup.py** - MySQL Binlog 实时备份
```bash
python3 binlog_backup.py
```
功能：
- 从 MySQL 配置文件自动读取 binlog 路径
- 准实时备份已完成的 binlog 文件（当前写入的除外）
- 使用 gzip 压缩备份
- 默认检查间隔 30 秒

### 备份管理工具

**backup_cleaner.py** - 清理过期备份文件
```bash
python3 backup_cleaner.py
```
功能：
- 基于日期保留策略：近两个月全保留，超过两个月仅保留每月 1/11/21 日
- 检查文件是否被占用（使用 lsof）
- 支持多个备份目录
- 日志记录到 `/tmp/cleaner.log`

**update_backup.py** - 生成备份配置更新
```bash
python3 update_backup.py 192.168.0.10
```
功能：
- 从 MySQL 数据库查询集群信息
- 从 etcd 获取当前 HA 配置
- 生成需要删除/新增备份实例的配置（输出到 `/tmp/xx`）
- 自动排除 mdbp 集群

**resync.py** - 备份文件同步和补全
```bash
python3 resync.py
```
功能：
- 同步本地备份到 GFS（最近 4 天的文件）
- 每月 1 号补全上个月的 1/11/21 日备份（用向后最近一天的替代）

**rdb/xtrabackup_sync.py** - 远程 Xtrabackup 备份与恢复工具
```bash
python3 rdb/xtrabackup_sync.py <host_a> <host_b> [--dry-run]
```
功能：
- 远程协调 A/B 机器执行 MySQL 8.0.25 物理备份与恢复
- 支持自动读取 `rdb/config.yaml` 凭据配置
- 包含服务停止、子目录清理重建、网络传输、prepare、copy-back 与服务启动全流程
- 支持 `--dry-run` 预览与交互式/自动临时文件清理

### 配置生成工具

**generate_config.py** - 从源文件生成复制配置
```bash
python3 generate_config.py -i source.txt -o output.txt
```
输入格式：
```
127.0.0.1 127.0.0.2 127.0.0.3
128.0.0.1 128.0.0.2
```
输出格式：
```
127.0.0.1
127.0.0.2 repl_master_ip=127.0.0.1
127.0.0.3 repl_master_ip=127.0.0.2
```

**gen_conf/generate_config.py** - 生成带数据库名的配置
```bash
python3 gen_conf/generate_config.py -i source.txt
```
输出格式：
```
#db1
127.0.0.1
127.0.0.2 rep_master_ip=127.0.0.1
```

**gen_conf/generate_dns.py** - 生成 DNS A 记录
```bash
python3 gen_conf/generate_dns.py -i source.txt --prefix ylp1- --domain local.xx.int. --pad 2
```

### 远程执行工具

**ssh.py** - SSH 远程执行和文件传输
```python
from ssh import ServerRemoteExecute

with ServerRemoteExecute('instance_name') as remote:
    ret_code, output = remote.execute('df -h', sudo=True)
    remote.scp_(Path('/tmp/script.sh'))
```
功能：
- 支持密码、密钥、外部认证
- 使用 ControlMaster 复用连接
- 支持 sudo 执行
- 自动重连机制

### 运维脚本

**downgrade.py** - MySQL HA 集群降级
```bash
python3 downgrade.py arbit_server
```
功能：
- 从 etcd 获取 HA 集群信息
- 检查 MySQL 健康状态
- 故障时杀死 HA 监控脚本

**col_count.py** - 批量查询 MySQL 表行数
```bash
python3 col_count.py --threshold 5000000 --workers 8 --output large_tables.csv
```
功能：
- 从源数据库查询 IP 列表（通过环境变量配置）
- 并发连接多个 MySQL 实例
- 查询 `information_schema.tables` 获取表行数估算值
- 输出超过阈值的表信息

## 架构说明

### 递归查找机制
多个工具使用递归查找逻辑：
- **向上查找**：从 `SHOW SLAVE STATUS/SHOW REPLICA STATUS` 获取 Master 信息，递归直到主库
- **向下查找**：从 `SHOW SLAVE HOSTS/SHOW REPLICAS` 获取 Slaves 列表，递归遍历

### 数据库连接
所有工具使用 `pymysql` 连接 MySQL，基本模式：
```python
conn = pymysql.connect(host=host, user=user, password=password, port=port,
                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
```

### 安全机制
- `gtid_reset.py` 在执行重置前先设置只读模式
- `mysql_binlog_cleaner.py` 检查主库是否可写
- `backup_cleaner.py` 使用 lsof 检查文件占用
- 所有工具都有详细的日志记录

## 常见任务

### 检查 GTID 一致性
```bash
python3 compare_gtid.py master_ip slave_ip --user root --password pwd
```

### 扫描 MySQL 拓扑
```bash
python3 mysql_topology.py start_ip
```

### 清理过期 binlog
```bash
python3 mysql_binlog_cleaner.py any_instance_ip
```

### 清理过期备份
```bash
python3 backup_cleaner.py
```

### 重置 GTID 并恢复复制
```bash
python3 gtid_reset.py --hosts ip1,ip2 --user root --password pwd \
    --replica-user repl --replica-password replpwd
```

## 注意事项

1. **数据库凭据**：敏感信息存储在 `.env` 文件中，不要提交到版本控制（已添加到 .gitignore）
2. **硬编码配置**：部分脚本包含硬编码的数据库连接信息，使用前请检查并修改
3. **MySQL 版本兼容**：工具支持 MySQL 5.x 和 8.x，优先使用 8.x 新语法（`SHOW REPLICA STATUS`）
4. **测试环境**：在生产环境使用前，建议先用 `--dry-run` 参数测试
5. **日志文件**：各工具的日志文件位置见各脚本顶部的 LOG_FILE 配置
