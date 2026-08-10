#!/usr/bin/env python3
"""MySQL 8.0.32 数据库迁移工具 (自包含脚本).

本脚本用于 MySQL 8.0.32 数据库在多源-多目标场景下的迁移。
核心功能：
1. 迁移数据库用户及权限 (独立导出为 01_users.sql)；
2. 迁移数据库表结构 (导出为 02_schema_<dbname>.sql)；
3. 默认不迁移数据，可通过参数/配置按需开启数据迁移 (导出为 03_data_<dbname>.sql)；
4. 导出时使用 mysqldump 显式添加 --set-gtid-purged=OFF；
5. 通过配置文件黑白名单规则过滤库名与用户名；
6. 支持仅导出 SQL 文件 (--dump-only) 与直接在线同步至目标库；
7. 不依赖上层目录任何代码。

Usage:
    python3 migrate.py [-c CONFIG] [--include-data] [--dump-only] [--dry-run] [--source SOURCE_IP]
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql
from pymysql.cursors import DictCursor
import yaml

# 配置默认日志输出格式
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MySQLMigrator")

# 系统默认忽略的数据库集合（不可迁移）
SYSTEM_DATABASES: Set[str] = {
    "information_schema",
    "performance_schema",
    "mysql",
    "sys",
}

# 系统默认忽略的用户账号集合（不进行导出与迁移）
SYSTEM_USER_NAMES: Set[str] = {
    "root",
    "mysql.sys",
    "mysql.session",
    "mysql.infoschema",
    "debian-sys-maint",
}


class MigrationConfig:
    """迁移配置解析与管理类."""

    def __init__(self, config_data: Dict[str, Any], cli_args: argparse.Namespace) -> None:
        """初始化迁移配置类.

        Args:
            config_data: 从 YAML 配置文件读取的字典数据.
            cli_args: 命令行解析得到的参数对象.
        """
        self.common: Dict[str, Any] = config_data.get("common", {})
        self.raw_migrations: List[Dict[str, Any]] = config_data.get("migrations", [])
        self.filter_config: Dict[str, Any] = config_data.get("filter", {})
        self.output_dir: str = cli_args.output_dir or config_data.get("output_dir", "./dumps")

        self.cli_include_data: bool = cli_args.include_data
        self.dump_only: bool = cli_args.dump_only
        self.dry_run: bool = cli_args.dry_run
        self.filter_source: Optional[str] = cli_args.source
        self.filter_target: Optional[str] = cli_args.target

        # 整理黑白名单集合
        self.ignore_databases: Set[str] = set(self.filter_config.get("ignore_databases") or [])
        self.include_databases: Set[str] = set(self.filter_config.get("include_databases") or [])
        self.ignore_users: Set[str] = set(self.filter_config.get("ignore_users") or [])
        self.include_users: Set[str] = set(self.filter_config.get("include_users") or [])

    def get_resolved_migrations(self) -> List[Dict[str, Any]]:
        """计算合并通用凭据与参数后的完整迁移任务列表.

        Returns:
            处理完成的包含源/目标数据库全量参数及开关的字典列表.
        """
        resolved: List[Dict[str, Any]] = []

        for item in self.raw_migrations:
            src_host = item.get("source")
            tgt_host = item.get("target")

            if not src_host or not tgt_host:
                logger.warning("跳过无效的迁移配置条目（缺少 source 或 target）: %s", item)
                continue

            # 支持按命令行参数过滤指定的 IP 对
            if self.filter_source and src_host != self.filter_source:
                continue
            if self.filter_target and tgt_host != self.filter_target:
                continue

            # 数据迁移开关逻辑：项级配置 > 命令行全局覆盖
            item_include_data = item.get("include_data")
            if self.cli_include_data:
                include_data = True
            elif item_include_data is not None:
                include_data = bool(item_include_data)
            else:
                include_data = False

            # 合并源凭据与目标凭据
            source_params = {
                "host": src_host,
                "port": item.get("port", self.common.get("port", 3306)),
                "user": item.get("user", self.common.get("user", "root")),
                "password": item.get("password", self.common.get("password", "")),
            }

            target_params = {
                "host": tgt_host,
                "port": item.get("target_port", item.get("port", self.common.get("port", 3306))),
                "user": item.get("target_user", item.get("user", self.common.get("user", "root"))),
                "password": item.get("target_password", item.get("password", self.common.get("password", ""))),
            }

            resolved.append(
                {
                    "source": source_params,
                    "target": target_params,
                    "include_data": include_data,
                }
            )

        return resolved


class UserMigrator:
    """数据库用户与权限迁移处理器."""

    def __init__(self, connection_params: Dict[str, Any]) -> None:
        """初始化 UserMigrator 实例.

        Args:
            connection_params: PyMySQL 连接所需的字典参数 (host, port, user, password).
        """
        self.params = connection_params

    def extract_user_grants_sql(
        self, ignore_users: Set[str], include_users: Set[str], dry_run: bool = False
    ) -> List[str]:
        """连接源数据库提取非忽略用户的创建与授权 SQL 语句.

        Args:
            ignore_users: 忽略的用户白名单之外的黑名单用户集合.
            include_users: 白名单用户集合（留空或仅包含 * 表示提取全部非忽略用户）.
            dry_run: 是否为试运行模式.

        Returns:
            生成的包含 CREATE USER 和 GRANT 语句的 SQL 文本行列表.
        """
        sql_lines: List[str] = [
            "-- ==================================================================",
            "-- MySQL 8.0.32 用户与权限迁移 DDL (自动生成)",
            "-- ==================================================================",
            "SET FOREIGN_KEY_CHECKS = 0;",
            "",
        ]

        try:
            connection = pymysql.connect(
                host=self.params["host"],
                port=int(self.params["port"]),
                user=self.params["user"],
                password=str(self.params["password"]),
                charset="utf8mb4",
                cursorclass=DictCursor,
                connect_timeout=3,
            )
        except Exception as err:
            if dry_run:
                logger.warning("[Dry-Run] 源库 (%s) 连接失败: %s。使用模拟用户做试运行预览。", self.params["host"], err)
                sql_lines.append("-- [Dry-Run] CREATE USER IF NOT EXISTS 'app_user'@'%';")
                sql_lines.append("-- [Dry-Run] GRANT ALL PRIVILEGES ON `app_db`.* TO 'app_user'@'%';")
                sql_lines.append("SET FOREIGN_KEY_CHECKS = 1;")
                return sql_lines
            raise err

        try:
            with connection.cursor() as cursor:
                # 查询所有用户与主机
                cursor.execute("SELECT User, Host FROM mysql.user")
                all_users = cursor.fetchall()

                for u_row in all_users:
                    username = u_row["User"]
                    hostname = u_row["Host"]

                    # 1. 系统内置用户过滤
                    if username in SYSTEM_USER_NAMES:
                        continue

                    # 2. 黑名单用户过滤
                    if username in ignore_users:
                        logger.info("忽略黑名单用户: '%s'@'%s'", username, hostname)
                        continue

                    # 3. 白名单用户过滤 (白名单非空且未显式指定 '*' 时生效)
                    if include_users and "*" not in include_users and username not in include_users:
                        logger.info("跳过非白名单用户: '%s'@'%s'", username, hostname)
                        continue

                    logger.info("正在提取用户权限: '%s'@'%s'...", username, hostname)
                    sql_lines.append(f"-- User: '{username}'@'{hostname}'")

                    # 提取 CREATE USER 语句
                    try:
                        cursor.execute(f"SHOW CREATE USER `{username}`@`{hostname}`")
                        create_res = cursor.fetchone()
                        if create_res:
                            # 提取 CREATE USER DDL 结果
                            create_sql = list(create_res.values())[0]
                            # 修改为安全创建 IF NOT EXISTS 格式
                            if create_sql.startswith("CREATE USER"):
                                create_sql = create_sql.replace("CREATE USER", "CREATE USER IF NOT EXISTS", 1)
                            sql_lines.append(f"{create_sql};")
                    except Exception as err:
                        logger.warning("无法提取 CREATE USER '%s'@'%s': %s", username, hostname, err)

                    # 提取 SHOW GRANTS 语句
                    try:
                        cursor.execute(f"SHOW GRANTS FOR `{username}`@`{hostname}`")
                        grants_res = cursor.fetchall()
                        for g_row in grants_res:
                            grant_sql = list(g_row.values())[0]
                            sql_lines.append(f"{grant_sql};")
                    except Exception as err:
                        logger.warning("无法提取 SHOW GRANTS FOR '%s'@'%s': %s", username, hostname, err)

                    sql_lines.append("")
        finally:
            connection.close()

        sql_lines.append("SET FOREIGN_KEY_CHECKS = 1;")
        return sql_lines


class DatabaseMigrator:
    """表结构与数据迁移处理器 (基于 mysqldump)."""

    def __init__(self, source_params: Dict[str, Any], target_params: Dict[str, Any]) -> None:
        """初始化 DatabaseMigrator 实例.

        Args:
            source_params: 源数据库连接参数.
            target_params: 目标数据库连接参数.
        """
        self.src = source_params
        self.tgt = target_params

    def get_target_databases(self, ignore_dbs: Set[str], include_dbs: Set[str], dry_run: bool = False) -> List[str]:
        """获取源库中需要迁移的数据库名称列表.

        Args:
            ignore_dbs: 忽略的数据库黑名单集合.
            include_dbs: 包含的数据库白名单集合.
            dry_run: 是否为试运行模式.

        Returns:
            满足过滤条件的数据库名列表.
        """
        try:
            connection = pymysql.connect(
                host=self.src["host"],
                port=int(self.src["port"]),
                user=self.src["user"],
                password=str(self.src["password"]),
                charset="utf8mb4",
                cursorclass=DictCursor,
                connect_timeout=3,
            )
        except Exception as err:
            if dry_run:
                logger.warning("[Dry-Run] 源库 (%s) 连接失败: %s。使用模拟库列表进行预览。", self.src["host"], err)
                return ["app_db", "order_db"]
            raise err

        target_dbs: List[str] = []
        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                rows = cursor.fetchall()
                for r in rows:
                    db_name = r["Database"]
                    # 1. 过滤系统库
                    if db_name in SYSTEM_DATABASES:
                        continue
                    # 2. 过滤黑名单
                    if db_name in ignore_dbs:
                        logger.info("忽略黑名单数据库: %s", db_name)
                        continue
                    # 3. 过滤白名单
                    if include_dbs and "*" not in include_dbs and db_name not in include_dbs:
                        logger.info("跳过非白名单数据库: %s", db_name)
                        continue
                    target_dbs.append(db_name)
        finally:
            connection.close()

        return target_dbs

    def _build_mysqldump_base_cmd(self, db_name: str) -> List[str]:
        """构建 mysqldump 命令的通用前缀部分.

        Args:
            db_name: 目标的数据库名.

        Returns:
            包含 mysqldump 命令行基础选项的字符串列表.
        """
        return [
            "mysqldump",
            f"--host={self.src['host']}",
            f"--port={self.src['port']}",
            f"--user={self.src['user']}",
            f"--password={self.src['password']}",
            "--set-gtid-purged=OFF",  # 核心要求：迁移时不保留/设置 gtid_purged
            "--single-transaction",
            "--column-statistics=0",
            "--triggers",
            "--databases",
            db_name,
        ]

    def dump_schema(self, db_name: str, output_path: str, dry_run: bool) -> bool:
        """使用 mysqldump 导出数据库表结构 (不含数据).

        Args:
            db_name: 数据库名称.
            output_path: SQL 导出的目标路径.
            dry_run: 是否仅试运行预览命令.

        Returns:
            导出成功返回 True，失败或报错返回 False.
        """
        cmd = self._build_mysqldump_base_cmd(db_name)
        cmd.insert(5, "--no-data")  # 仅导出结构

        # 屏蔽命令行打印中的明文密码
        safe_cmd = [c if not c.startswith("--password=") else "--password=******" for c in cmd]
        logger.info("执行表结构导出: %s -> %s", " ".join(safe_cmd), output_path)

        if dry_run:
            return True

        try:
            with open(output_path, "w", encoding="utf8") as f:
                res = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, universal_newlines=True, check=True)
            return True
        except subprocess.CalledProcessError as err:
            logger.error("导出表结构失败 [%s]: %s", db_name, err.stderr)
            return False

    def dump_data(self, db_name: str, output_path: str, dry_run: bool) -> bool:
        """使用 mysqldump 导出数据库数据 (不含表结构创建).

        Args:
            db_name: 数据库名称.
            output_path: SQL 导出的目标路径.
            dry_run: 是否仅试运行预览命令.

        Returns:
            导出成功返回 True，失败或报错返回 False.
        """
        cmd = self._build_mysqldump_base_cmd(db_name)
        cmd.insert(5, "--no-create-info")  # 仅导出数据，不重造表结构

        safe_cmd = [c if not c.startswith("--password=") else "--password=******" for c in cmd]
        logger.info("执行数据导出: %s -> %s", " ".join(safe_cmd), output_path)

        if dry_run:
            return True

        try:
            with open(output_path, "w", encoding="utf8") as f:
                res = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, universal_newlines=True, check=True)
            return True
        except subprocess.CalledProcessError as err:
            logger.error("导出数据失败 [%s]: %s", db_name, err.stderr)
            return False

    def import_sql_to_target(self, sql_file_path: str, dry_run: bool) -> bool:
        """使用 mysql 客户端命令行将 SQL 文件导入到目标数据库中.

        Args:
            sql_file_path: 要导入的 SQL 文件绝对或相对路径.
            dry_run: 是否仅试运行预览命令.

        Returns:
            导入成功返回 True，失败或报错返回 False.
        """
        cmd = [
            "mysql",
            f"--host={self.tgt['host']}",
            f"--port={self.tgt['port']}",
            f"--user={self.tgt['user']}",
            f"--password={self.tgt['password']}",
        ]

        safe_cmd = [c if not c.startswith("--password=") else "--password=******" for c in cmd]
        logger.info("执行目标库导入: %s < %s", " ".join(safe_cmd), sql_file_path)

        if dry_run:
            return True

        try:
            with open(sql_file_path, "r", encoding="utf8") as f:
                subprocess.run(cmd, stdin=f, stderr=subprocess.PIPE, universal_newlines=True, check=True)
            return True
        except subprocess.CalledProcessError as err:
            logger.error("导入 SQL 文件失败 [%s]: %s", sql_file_path, err.stderr)
            return False


class MigrationOrchestrator:
    """迁移任务编排控制主类."""

    def __init__(self, config: MigrationConfig) -> None:
        """初始化 MigrationOrchestrator 实例.

        Args:
            config: 已解析的 MigrationConfig 配置类实例.
        """
        self.config = config

    def run(self) -> None:
        """执行全部迁移任务的编排入口."""
        migrations = self.config.get_resolved_migrations()
        if not migrations:
            logger.warning("未找到需要执行的有效迁移任务。请检查 config.yaml 属性。")
            return

        logger.info("开始执行数据库迁移任务流程，共计 %d 组主机映射...", len(migrations))

        for idx, task in enumerate(migrations, 1):
            src_params = task["source"]
            tgt_params = task["target"]
            include_data = task["include_data"]

            logger.info("==================================================================")
            logger.info(
                "[%d/%d] 启动迁移: 源主机(%s:%s) -> 目标主机(%s:%s) [是否迁移数据: %s]",
                idx,
                len(migrations),
                src_params["host"],
                src_params["port"],
                tgt_params["host"],
                tgt_params["port"],
                include_data,
            )
            logger.info("==================================================================")

            # 创建以源主机 IP 命名隔离的 dump 子目录
            instance_dump_dir = os.path.join(self.config.output_dir, src_params["host"])
            if not self.config.dry_run:
                os.makedirs(instance_dump_dir, exist_ok=True)

            # 1. 独立导出与导入用户 SQL
            user_migrator = UserMigrator(src_params)
            user_sql_file = os.path.join(instance_dump_dir, "01_users.sql")

            try:
                sql_lines = user_migrator.extract_user_grants_sql(
                    self.config.ignore_users, self.config.include_users, self.config.dry_run
                )
                if not self.config.dry_run:
                    with open(user_sql_file, "w", encoding="utf8") as f:
                        f.write("\n".join(sql_lines) + "\n")
                    logger.info("用户权限 SQL 成功导出至: %s", user_sql_file)
                else:
                    logger.info("[Dry-Run] 预览导出用户权限 SQL -> %s", user_sql_file)
            except Exception as err:
                logger.error("导出用户权限失败，源主机 (%s): %s", src_params["host"], err)

            db_migrator = DatabaseMigrator(src_params, tgt_params)

            # 在非 dry-run 且不为 dump-only 模式时提前同步导入用户 SQL
            if not self.config.dump_only and not self.config.dry_run:
                if os.path.exists(user_sql_file):
                    db_migrator.import_sql_to_target(user_sql_file, False)

            # 2. 导出与导入数据库结构及数据
            try:
                target_dbs = db_migrator.get_target_databases(
                    self.config.ignore_databases, self.config.include_databases, self.config.dry_run
                )
                logger.info("包含的需要迁移的数据库清单: %s", target_dbs)
            except Exception as err:
                logger.error("查询源主机 (%s) 数据库清单失败: %s", src_params["host"], err)
                continue

            for db_name in target_dbs:
                schema_file = os.path.join(instance_dump_dir, f"02_schema_{db_name}.sql")
                # 导出表结构
                success = db_migrator.dump_schema(db_name, schema_file, self.config.dry_run)

                # 导入表结构到目标库
                if success and not self.config.dump_only:
                    db_migrator.import_sql_to_target(schema_file, self.config.dry_run)

                # 导出与导入数据（仅当 include_data=True 时）
                if include_data:
                    data_file = os.path.join(instance_dump_dir, f"03_data_{db_name}.sql")
                    d_success = db_migrator.dump_data(db_name, data_file, self.config.dry_run)

                    if d_success and not self.config.dump_only:
                        db_migrator.import_sql_to_target(data_file, self.config.dry_run)

        logger.info("==================================================================")
        logger.info("所有迁移映射任务处理完毕！")


def parse_cli_args() -> argparse.Namespace:
    """构建与解析命令行输入参数.

    Returns:
        解析后的 Namespace 命令行参数对象.
    """
    parser = argparse.ArgumentParser(
        description="MySQL 8.0.32 自包含数据库迁移工具 (支持用户/结构/数据迁移与过滤)"
    )

    parser.add_argument(
        "-c",
        "--config",
        default=os.path.join(os.path.dirname(__file__), "config.yaml"),
        help="配置文件 YAML 路径 (默认: gen_dump/config.yaml)",
    )
    parser.add_argument(
        "--include-data",
        action="store_true",
        help="强制全局开启数据迁移 (覆盖配置文件中 include_data 设置)",
    )
    parser.add_argument(
        "--dump-only",
        action="store_true",
        help="仅导出 SQL 备份文件至 output_dir，不直接连接目标库进行导入",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="试运行模式，仅打印将要执行的 SQL 提取与 mysqldump 命令",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=None,
        help="SQL 导出根目录 (覆盖配置文件中的 output_dir 参数)",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="精确筛选仅执行指定源 IP (source_host) 的迁移映射",
    )
    parser.add_argument(
        "--target",
        default=None,
        help="精确筛选仅执行指定目标 IP (target_host) 的迁移映射",
    )

    return parser.parse_args()


def main() -> None:
    """程序入口主函数."""
    args = parse_cli_args()

    if not os.path.exists(args.config):
        logger.error("配置文件不存在: %s", args.config)
        sys.exit(1)

    try:
        with open(args.config, "r", encoding="utf8") as f:
            config_data = yaml.safe_load(f) or {}
    except Exception as err:
        logger.error("解析配置文件失败 [%s]: %s", args.config, err)
        sys.exit(1)

    config = MigrationConfig(config_data, args)
    orchestrator = MigrationOrchestrator(config)
    orchestrator.run()


if __name__ == "__main__":
    main()
