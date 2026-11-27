#!/usr/bin/env python3
"""MySQL 8.0.25 远程机器 Xtrabackup 备份与恢复工具 (单文件脚本).

本脚本用于在管理机上远程调用并协同完成从源主机 (A) 到目标主机 (B) 的 MySQL 8.0.25
物理备份、网络同步、数据准备与恢复操作。

核心流程：
1. 从配置文件读取 MySQL 连接凭据 (rdb: username, password, port)；
2. 校验管理机到 A/B 两台机器的 SSH 联通性；
3. 在 B 机器停止服务 (ommagentdbmoni -stop && dbmoni -stop)；
4. 在 B 机器清理并重建数据子目录 (binlog, data, redo, relaylog, tmp, undo)；
5. 在 A 机器使用 xtrabackup 备份 MySQL 数据文件到临时目录；
6. 在 A 机器通过 rsync 传输备份至 B 机器；
7. 在 B 机器执行 xtrabackup --prepare 进行数据准备；
8. 在 B 机器执行 xtrabackup --copy-back 恢复数据到数据目录；
9. 在 B 机器按顺序启动服务 (dbmoni -start && ommagentdbmoni -start)；
10. 清理 A 和 B 机器上的临时备份目录 (支持交互式确认或直接执行)。

Usage:
    python3 rdb/xtrabackup_sync.py <host_a> <host_b> [-c CONFIG] [--dry-run]
"""

import argparse
import datetime
import logging
import os
import subprocess
import sys
from typing import Any, Dict, List, Optional, Tuple

import yaml

# 导入 myssh 模块中的 run_remote 函数
try:
    from ymm.myssh import run_remote
except ImportError:
    # 支持在不同工作目录下查找根目录或当前目录中的 myssh
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from ymm.myssh import run_remote

# 配置全局日志格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("XtrabackupSync")


class RdbConfig:
    """RDB 数据库凭据配置解析与管理类."""

    def __init__(self, config_path: Optional[str] = None) -> None:
        """初始化配置管理器并加载 YAML 配置文件.

        Args:
            config_path: 配置文件路径，若为 None 则按默认候选路径自动查找。

        Raises:
            FileNotFoundError: 当未找到任何有效配置文件时抛出。
            ValueError: 当配置文件缺少必须的 rdb 字段时抛出。
        """
        self.config_path = self._locate_config(config_path)
        self.username: str = "root"
        self.password: str = ""
        self.port: int = 3306
        self._load_config()

    def _locate_config(self, specified_path: Optional[str]) -> str:
        """寻找存在的配置文件路径.

        Args:
            specified_path: 用户指定的配置文件路径。

        Returns:
            找到的有效配置文件绝对路径。

        Raises:
            FileNotFoundError: 无法找到有效配置文件。
        """
        if specified_path:
            abs_path = os.path.abspath(specified_path)
            if os.path.isfile(abs_path):
                return abs_path
            raise FileNotFoundError("未找到指定的配置文件: {}".format(specified_path))

        # 默认候选查找路径列表
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        candidate_paths = [
            os.path.join(script_dir, "config.yaml"),
            os.path.join(project_root, "rdb", "config.yaml"),
            os.path.join(project_root, "gdb", "config.yaml"),
            os.path.join(os.getcwd(), "config.yaml"),
        ]

        for path in candidate_paths:
            if os.path.isfile(path):
                return path

        raise FileNotFoundError(
            "未找到默认配置文件，请通过 -c/--config 指定。查找路径包括:\n  - {}".format(
                "\n  - ".join(candidate_paths)
            )
        )

    def _load_config(self) -> None:
        """读取并解析 YAML 配置文件中的 rdb 字段.

        Raises:
            ValueError: 缺少必须字段或配置格式异常时抛出。
        """
        logger.info("加载配置文件: %s", self.config_path)
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict) or "rdb" not in data:
            raise ValueError("配置文件中缺少根级别 'rdb' 配置块: {}".format(self.config_path))

        rdb_data = data["rdb"]
        if not isinstance(rdb_data, dict):
            raise ValueError("配置文件中的 'rdb' 项必须为字典对象")

        self.username = str(rdb_data.get("username", "root"))
        self.password = str(rdb_data.get("password", ""))
        self.port = int(rdb_data.get("port", 3306))
        logger.info("已读取数据库配置: 用户名=%s, 端口=%d", self.username, self.port)


class RemoteExecutor:
    """远程 SSH 命令执行器封装类 (基于 myssh 模块)."""

    def __init__(
        self,
        ssh_user: str = "insight",
        ssh_port: int = 22,
        dry_run: bool = False,
        logger_instance: Optional[logging.Logger] = None,
    ) -> None:
        """初始化远程执行器.

        Args:
            ssh_user: SSH 登录用户名，默认 insight。
            ssh_port: SSH 服务端口，默认 22。
            dry_run: 是否为试运行模式（仅打印不实际执行）。
            logger_instance: 可选的日志记录器实例。
        """
        self.ssh_user = ssh_user
        self.ssh_port = ssh_port
        self.dry_run = dry_run
        self.logger = logger_instance or logger

    def run_ssh(self, host: str, command: str, check: bool = True) -> Tuple[int, str]:
        """在目标机器上通过 myssh.run_remote 执行指定命令.

        Args:
            host: 目标机器 IP 或主机名。
            command: 需要执行的 Shell 命令。
            check: 是否在返回非 0 状态码时抛出异常。

        Returns:
            元组 (returncode, output)。

        Raises:
            RuntimeError: 当命令执行失败且 check=True 时抛出。
        """
        cmd_display = "ssh -p {} {}@{} '{}'".format(self.ssh_port, self.ssh_user, host, command)
        logger.info("[远程执行 @ %s]: %s", host, command)

        if self.dry_run:
            logger.info("[Dry-Run] 跳过实际执行: %s", cmd_display)
            return (0, "")

        code, output = run_remote(
            cmd=command,
            host=host,
            port=self.ssh_port,
            user=self.ssh_user,
            logger=self.logger,
        )

        if code != 0:
            logger.error(
                "[%s 失败] 命令执行异常 (退出码: %d)\n输出内容: %s",
                host,
                code,
                output,
            )
            if check:
                raise RuntimeError(
                    "远程主机 {} 执行命令失败 (退出码: {}): {}\n{}".format(
                        host, code, command, output
                    )
                )
        return (code, output)


class XtrabackupSyncer:
    """Xtrabackup 备份与恢复全流程同步控制器."""

    SUB_DIRS: List[str] = ["binlog", "data", "redo", "relaylog", "tmp", "undo"]

    def __init__(
        self,
        host_a: str,
        host_b: str,
        config: RdbConfig,
        executor: RemoteExecutor,
        base_dir: str = "/data/goldendb/insight",
        clean_mode: str = "interactive",
    ) -> None:
        """初始化同步控制器.

        Args:
            host_a: 源机器 IP / 主机名。
            host_b: 目标机器 IP / 主机名。
            config: RDB 凭据配置对象。
            executor: 远程命令执行器。
            base_dir: Insight 根工作目录，默认 /data/goldendb/insight。
            clean_mode: 临时文件清理模式 ('interactive', 'auto', 'keep')。
        """
        self.host_a = host_a
        self.host_b = host_b
        self.config = config
        self.executor = executor
        self.base_dir = base_dir.rstrip("/")
        self.clean_mode = clean_mode

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        self.backup_tmp_dir = "{}/backup_tmp_{}".format(self.base_dir, timestamp)
        self.data_root = "{}/data".format(self.base_dir)
        self.mysql_data_dir = "{}/data/data".format(self.base_dir)

    def check_connectivity(self) -> None:
        """测试管理机到源端 A 和目标端 B 的 SSH 免密联通性.

        Raises:
            RuntimeError: 当任意一台机器连接不可达时抛出。
        """
        logger.info("=== 步骤 0: 检查节点 A (%s) 与节点 B (%s) 的 SSH 联通性 ===", self.host_a, self.host_b)
        self.executor.run_ssh(self.host_a, "echo SSH_CONNECTIVITY_OK")
        self.executor.run_ssh(self.host_b, "echo SSH_CONNECTIVITY_OK")
        logger.info("SSH 联通性检查通过。")

    def stop_services_on_b(self) -> None:
        """在目标机器 B 上停止监控与数据库进程 (ommagentdbmoni -stop && dbmoni -stop)."""
        logger.info("=== 步骤 1: 在目标机器 B (%s) 停止服务 ===", self.host_b)
        stop_cmd = "ommagentdbmoni -stop && dbmoni -stop"
        self.executor.run_ssh(self.host_b, stop_cmd)
        logger.info("目标机器 B 上服务已成功停止。")

    def recreate_data_directories_on_b(self) -> None:
        """在目标机器 B 上删除并重建指定的子目录."""
        logger.info("=== 步骤 2: 在目标机器 B (%s) 清理并重建数据子目录 ===", self.host_b)
        dir_paths = ["{}/{}".format(self.data_root, d) for d in self.SUB_DIRS]
        paths_str = " ".join(dir_paths)

        # 依次删除并重建目录
        clean_recreate_cmd = "rm -rf {} && mkdir -p {}".format(paths_str, paths_str)
        self.executor.run_ssh(self.host_b, clean_recreate_cmd)
        logger.info("数据目录 (%s) 已清理并完成重建。", ", ".join(self.SUB_DIRS))

    def backup_on_a(self) -> None:
        """在源机器 A 上执行 xtrabackup 物理备份."""
        logger.info("=== 步骤 3: 在源机器 A (%s) 执行 xtrabackup 备份 ===", self.host_a)
        # 确保备份临时目录父目录存在并创建备份目录
        mkdir_cmd = "mkdir -p {}".format(self.backup_tmp_dir)
        self.executor.run_ssh(self.host_a, mkdir_cmd)

        # 构造 xtrabackup 备份命令
        xtrabackup_cmd = (
            "xtrabackup --backup --target-dir={} --datadir={} "
            "--host=127.0.0.1 --port={} --user={} --password='{}' "
            "--no-server-version-check"
        ).format(
            self.backup_tmp_dir,
            self.mysql_data_dir,
            self.config.port,
            self.config.username,
            self.config.password,
        )

        self.executor.run_ssh(self.host_a, xtrabackup_cmd)
        logger.info("源机器 A 备份完成，输出目录: %s", self.backup_tmp_dir)

    def transfer_to_b(self) -> None:
        """在源机器 A 上通过 rsync 将备份目录传输至目标机器 B."""
        logger.info("=== 步骤 4: 从源机器 A (%s) 传输备份至目标机器 B (%s) ===", self.host_a, self.host_b)
        # 先确保 B 机器上存在临时目录的父级目录
        self.executor.run_ssh(self.host_b, "mkdir -p {}".format(self.base_dir))

        # A 机器到 B 机器已免密，通过 rsync 进行同步
        rsync_cmd = (
            "rsync -avzP {}/ {}@{}:{}/"
        ).format(
            self.backup_tmp_dir,
            self.executor.ssh_user,
            self.host_b,
            self.backup_tmp_dir,
        )

        self.executor.run_ssh(self.host_a, rsync_cmd)
        logger.info("备份数据传输完成: %s -> %s", self.host_a, self.host_b)

    def prepare_on_b(self) -> None:
        """在目标机器 B 上执行 xtrabackup --prepare 数据准备."""
        logger.info("=== 步骤 5: 在目标机器 B (%s) 执行 xtrabackup --prepare ===", self.host_b)
        prepare_cmd = "xtrabackup --prepare --target-dir={}".format(self.backup_tmp_dir)
        self.executor.run_ssh(self.host_b, prepare_cmd)
        logger.info("目标机器 B 数据准备 (prepare) 完成。")

    def restore_on_b(self) -> None:
        """在目标机器 B 上执行 xtrabackup --copy-back 恢复数据."""
        logger.info("=== 步骤 6: 在目标机器 B (%s) 恢复数据至数据目录 ===", self.host_b)
        # 恢复前确保目标数据目录存在
        self.executor.run_ssh(self.host_b, "mkdir -p {}".format(self.mysql_data_dir))

        copy_back_cmd = (
            "xtrabackup --copy-back --target-dir={} --datadir={}"
        ).format(
            self.backup_tmp_dir,
            self.mysql_data_dir,
        )

        self.executor.run_ssh(self.host_b, copy_back_cmd)
        logger.info("目标机器 B 数据恢复完成，目标路径: %s", self.mysql_data_dir)

    def start_services_on_b(self) -> None:
        """在目标机器 B 上按顺序启动服务 (dbmoni -start && ommagentdbmoni -start)."""
        logger.info("=== 步骤 7: 在目标机器 B (%s) 启动服务 ===", self.host_b)
        start_cmd = "dbmoni -start && ommagentdbmoni -start"
        self.executor.run_ssh(self.host_b, start_cmd)
        logger.info("目标机器 B 上服务启动成功 (dbmoni && ommagentdbmoni)。")

    def clean_temporary_backups(self) -> None:
        """根据配置模式清理 A 和 B 机器上的临时备份目录."""
        logger.info("=== 步骤 8: 清理临时备份目录 ===")
        logger.info("待清理目录路径: %s (机器 A: %s, 机器 B: %s)", self.backup_tmp_dir, self.host_a, self.host_b)

        if self.clean_mode == "keep":
            logger.info("根据配置 [keep 模式]，保留 A 和 B 上的临时备份目录。")
            return

        should_clean = False
        if self.clean_mode == "auto":
            should_clean = True
        elif self.clean_mode == "interactive":
            if self.executor.dry_run:
                logger.info("[Dry-Run] 交互模式下默认模拟清理操作。")
                should_clean = True
            else:
                try:
                    prompt = "是否删除 A 和 B 机器上的临时备份目录 ({})? [y/N]: ".format(self.backup_tmp_dir)
                    user_input = input(prompt).strip().lower()
                    if user_input in ("y", "yes"):
                        should_clean = True
                    else:
                        logger.info("用户选择保留临时备份文件。")
                except (EOFError, KeyboardInterrupt):
                    logger.info("\n取消删除临时备份文件。")
                    return

        if should_clean:
            clean_cmd = "rm -rf {}".format(self.backup_tmp_dir)
            logger.info("正在清理机器 A 上的临时目录: %s", self.backup_tmp_dir)
            self.executor.run_ssh(self.host_a, clean_cmd, check=False)
            logger.info("正在清理机器 B 上的临时目录: %s", self.backup_tmp_dir)
            self.executor.run_ssh(self.host_b, clean_cmd, check=False)
            logger.info("临时备份目录清理完成。")

    def run(self) -> None:
        """执行完整的备份恢复同步流程."""
        logger.info("==================================================================")
        logger.info("开始执行 MySQL 8.0.25 远程备份与恢复任务")
        logger.info("源主机 (A): %s", self.host_a)
        logger.info("目标主机 (B): %s", self.host_b)
        logger.info("基础目录: %s", self.base_dir)
        logger.info("清理模式: %s", self.clean_mode)
        logger.info("Dry-Run 模式: %s", self.executor.dry_run)
        logger.info("==================================================================")

        start_time = datetime.datetime.now()

        # 顺序执行各个业务步骤
        self.check_connectivity()
        self.stop_services_on_b()
        self.recreate_data_directories_on_b()
        self.backup_on_a()
        self.transfer_to_b()
        self.prepare_on_b()
        self.restore_on_b()
        self.start_services_on_b()
        self.clean_temporary_backups()

        elapsed = datetime.datetime.now() - start_time
        logger.info("==================================================================")
        logger.info("MySQL 备份与恢复任务执行完毕！总耗时: %s", elapsed)
        logger.info("==================================================================")


def parse_args() -> argparse.Namespace:
    """解析命令行参数.

    Returns:
        解析后的命令行命名空间对象。
    """
    parser = argparse.ArgumentParser(
        description="MySQL 8.0.25 远程机器 Xtrabackup 备份与恢复工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""示例用法:
  python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11
  python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 -c rdb/config.yaml --auto-clean
  python3 rdb/xtrabackup_sync.py 192.168.1.10 192.168.1.11 --dry-run
""",
    )

    parser.add_argument("host_a", metavar="HOST_A", help="源主机 IP 或主机名 (备份发起端)")
    parser.add_argument("host_b", metavar="HOST_B", help="目标主机 IP 或主机名 (恢复目标端)")

    parser.add_argument(
        "-c",
        "--config",
        dest="config_path",
        default=None,
        help="配置文件路径 (默认自动查找 rdb/config.yaml 或 gdb/config.yaml)",
    )

    parser.add_argument(
        "--ssh-user",
        dest="ssh_user",
        default="insight",
        help="SSH 远程登录用户名 (默认: insight)",
    )

    parser.add_argument(
        "--ssh-port",
        dest="ssh_port",
        type=int,
        default=22,
        help="SSH 远程端口 (默认: 22)",
    )

    parser.add_argument(
        "--base-dir",
        dest="base_dir",
        default="/data/goldendb/insight",
        help="GoldenDB Insight 基础工作目录 (默认: /data/goldendb/insight)",
    )

    clean_group = parser.add_mutually_exclusive_group()
    clean_group.add_argument(
        "--auto-clean",
        action="store_const",
        dest="clean_mode",
        const="auto",
        help="恢复完成后直接清理 A/B 机器上的临时备份目录 (不进行交互提示)",
    )
    clean_group.add_argument(
        "--no-clean",
        action="store_const",
        dest="clean_mode",
        const="keep",
        help="恢复完成后保留 A/B 机器上的临时备份目录",
    )
    clean_group.add_argument(
        "--interactive-clean",
        action="store_const",
        dest="clean_mode",
        const="interactive",
        help="恢复完成后交互式提示确认是否清理临时备份目录 (默认行为)",
    )
    parser.set_defaults(clean_mode="interactive")

    parser.add_argument(
        "--dry-run",
        action="store_true",
        dest="dry_run",
        help="试运行模式，仅打印将要执行的远程命令而不实际运行",
    )

    return parser.parse_args()


def main() -> None:
    """主程序入口函数."""
    args = parse_args()

    try:
        config = RdbConfig(config_path=args.config_path)
        executor = RemoteExecutor(
            ssh_user=args.ssh_user,
            ssh_port=args.ssh_port,
            dry_run=args.dry_run,
        )

        syncer = XtrabackupSyncer(
            host_a=args.host_a,
            host_b=args.host_b,
            config=config,
            executor=executor,
            base_dir=args.base_dir,
            clean_mode=args.clean_mode,
        )
        syncer.run()

    except KeyboardInterrupt:
        logger.warning("\n用户主动中断操作。")
        sys.exit(130)
    except Exception as e:
        logger.error("任务执行失败: %s", str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
