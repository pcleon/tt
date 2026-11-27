# MySQL 8.0.25 远程机器 Xtrabackup 备份与恢复工具 (`rdb/xtrabackup_sync.py`)

## 工具简介

单文件 Python 脚本（兼容 Python 3.6+），用于在管理机上通过 SSH 远程协同源节点（A）与目标节点（B），完成 MySQL 8.0.25 实例的物理热备份、跨机网络传输、数据准备（prepare）以及数据恢复（copy-back）。

---

## 核心流程

```
管理机 (Controller)
 │
 ├── 1. 读取 rdb/config.yaml 中的 rdb 配置 (username, password, port)
 ├── 2. 检查到 A 和 B 机器的 SSH 免密联通性
 ├── 3. 在 B 机器停止服务: ommagentdbmoni -stop && dbmoni -stop
 ├── 4. 在 B 机器清理并重建数据子目录:
 │      rm -rf /data/goldendb/insight/data/{binlog,data,redo,relaylog,tmp,undo}
 │      mkdir -p /data/goldendb/insight/data/{binlog,data,redo,relaylog,tmp,undo}
 ├── 5. 在 A 机器执行 xtrabackup 备份至 /data/goldendb/insight/backup_tmp_<timestamp>
 ├── 6. 在 A 机器执行 rsync 同步备份目录至 B 机器
 ├── 7. 在 B 机器执行 xtrabackup --prepare
 ├── 8. 在 B 机器执行 xtrabackup --copy-back 恢复至 /data/goldendb/insight/data/data
 ├── 9. 在 B 机器拉起服务: dbmoni -start && ommagentdbmoni -start
 └── 10. 根据清理模式清理 A、B 上的临时备份目录
```

---

## 配置文件 (`rdb/config.yaml`)

```yaml
rdb:
  username: "user"
  password: "password"
  port: 3309
```

---

## 命令与参数说明

### 基本用法

```bash
# 试运行（Dry-Run 模式，仅打印远程命令）
python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 --dry-run

# 默认执行（恢复完成后交互式提示是否清理临时备份目录）
python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11

# 自动清理临时目录
python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 --auto-clean

# 保留临时目录
python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 --no-clean

# 指定配置文件路径
python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 -c /path/to/config.yaml
```

### 参数列表

| 参数 | 说明 | 默认值 |
| :--- | :--- | :--- |
| `HOST_A` | 源主机 IP 或主机名（备份发起端） | 必填 |
| `HOST_B` | 目标主机 IP 或主机名（恢复目标端） | 必填 |
| `-c, --config` | 配置文件路径 | 自动探测 `rdb/config.yaml`、`gdb/config.yaml` 等 |
| `--ssh-user` | SSH 登录用户名 | `insight` |
| `--ssh-port` | SSH 端口 | `22` |
| `--base-dir` | Insight 根目录 | `/data/goldendb/insight` |
| `--interactive-clean` | 恢复后交互式确认清理 | 默认行为 |
| `--auto-clean` | 恢复后自动清理临时备份 | 否 |
| `--no-clean` | 恢复后保留临时备份 | 否 |
| `--dry-run` | 试运行模式 | 否 |
