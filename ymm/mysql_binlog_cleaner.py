#!/usr/bin/env python3
"""MySQL 二进制日志安全清理工具。

根据集群中任意已知节点向上追溯可写主库（支持双主成环破环判定），
递归探测所有从库并汇聚全局最小回放位点，安全执行 PURGE BINARY LOGS。
"""

import argparse
import logging
import logging.handlers
import re
import sys
from typing import Any, Dict, List, Optional, Set
import pymysql

# 配置日志（同时输出至轮转文件和终端控制台）
LOG_FILE = "/tmp/mysql_binlog_cleaner.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=3,
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)

# MySQL配置（硬编码，匹配目标环境）
MYSQL_CONFIG = {
    "host": "192.168.0.10",
    "user": "",
    "password": "",
    "port": 33306,
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
}

# 最大递归深度
MAX_RECURSION_DEPTH = 4


def connect_mysql(host: str) -> Optional[pymysql.Connection]:
    """连接到指定的 MySQL 服务器。

    Args:
        host: 目标服务器 IP 地址。

    Returns:
        Optional[pymysql.Connection]: 成功返回数据库连接对象，失败返回 None。
    """
    try:
        config = MYSQL_CONFIG.copy()
        config["host"] = host
        conn = pymysql.connect(**config)
        logging.info(f"成功连接到MySQL服务器: {host}")
        return conn
    except pymysql.Error as e:
        logging.error(f"连接MySQL服务器失败 {host}: {e}")
        return None


def query_replica_status(conn: pymysql.Connection) -> Optional[Dict[str, Any]]:
    """自适应执行 SHOW REPLICA/SLAVE STATUS 状态查询。

    优先使用 MySQL 8.0 规范语法，异常时自动降级至 MySQL 5.x 兼容语法。

    Args:
        conn: MySQL 连接对象。

    Returns:
        Optional[Dict[str, Any]]: 复制状态字典，若非副本或查询失败则返回 None。
    """
    try:
        with conn.cursor() as cursor:
            try:
                cursor.execute("SHOW REPLICA STATUS")
                return cursor.fetchone()
            except Exception:
                cursor.execute("SHOW SLAVE STATUS")
                return cursor.fetchone()
    except pymysql.Error as e:
        logging.error(f"查询复制状态失败: {e}")
        return None


def get_master_info(conn: pymysql.Connection) -> Optional[Dict[str, Any]]:
    """获取上层主库连接信息与当前中继主库日志位点。

    Args:
        conn: MySQL 连接对象。

    Returns:
        Optional[Dict[str, Any]]: 包含主库主机、端口及日志文件信息的字典，非备库返回 None。
    """
    result = query_replica_status(conn)
    if result:
        master_host = result.get("Source_Host") or result.get("Master_Host")
        master_port = result.get("Source_Port") or result.get("Master_Port")
        relay_log_file = result.get("Relay_Source_Log_File") or result.get("Relay_Master_Log_File")
        return {
            "master_host": master_host,
            "master_port": master_port,
            "relay_master_log_file": relay_log_file,
        }
    return None


def is_master_writable(conn: pymysql.Connection) -> bool:
    """检查主库是否可写（read_only 为 0）。

    Args:
        conn: MySQL 连接对象。

    Returns:
        bool: 可写返回 True，只读或异常返回 False。
    """
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@read_only AS read_only")
            result = cursor.fetchone()
            return bool(result and result["read_only"] == 0)
    except pymysql.Error as e:
        logging.error(f"检查主库可写状态失败: {e}")
        return False


def resolve_cycle_master(cycle_nodes: List[str]) -> Optional[str]:
    """在双主/多主成环节点中按可写状态（read_only == 0）破环选出活跃主库。

    Args:
        cycle_nodes: 构成闭环的节点 IP 列表。

    Returns:
        Optional[str]: 唯一可写的主库 IP；若无节点可写或存在双写冲突则返回 None。
    """
    writable_nodes = []
    for node_ip in cycle_nodes:
        conn = connect_mysql(node_ip)
        if not conn:
            logging.error(f"闭环节点 {node_ip} 无法连接，中止破环判定")
            return None
        try:
            if is_master_writable(conn):
                writable_nodes.append(node_ip)
        finally:
            conn.close()

    if len(writable_nodes) == 1:
        chosen_master = writable_nodes[0]
        logging.info(f"双主闭环破环成功，选定唯一可写主库: {chosen_master}")
        return chosen_master
    elif len(writable_nodes) == 0:
        logging.error(f"双主闭环中所有节点均处于只读状态（read_only=1），无法确定活跃主库: {cycle_nodes}")
        return None
    else:
        logging.error(f"检测到双写冲突，闭环中存在多个可写节点: {writable_nodes}")
        return None


def find_master_recursive(
    ip: str, depth: int = 0, path: Optional[List[str]] = None
) -> Optional[str]:
    """递归向上查找主库，并在检测到双主环路时按可写性破环。

    Args:
        ip: 当前探测节点 IP。
        depth: 当前递归深度。
        path: 已经走过的溯源路径列表。

    Returns:
        Optional[str]: 最终确认的可写主库 IP，失败或达到最大深度返回 None。
    """
    if path is None:
        path = []

    if depth >= MAX_RECURSION_DEPTH:
        logging.warning(f"达到最大递归深度 {MAX_RECURSION_DEPTH}，停止查找")
        return None

    current_path = path + [ip]
    conn = connect_mysql(ip)
    if not conn:
        return None

    try:
        master_info = get_master_info(conn)
        if not master_info or not master_info.get("master_host"):
            logging.info(f"找到主库: {ip}")
            return ip

        master_ip = master_info["master_host"]
        logging.info(f"从 {ip} 找到上层主库: {master_ip}")

        # 闭环检测：若上层主库已在路径中，说明检测到双主（Master-Master）或多主成环
        if master_ip in current_path:
            cycle_start = current_path.index(master_ip)
            cycle_nodes = current_path[cycle_start:]
            logging.warning(
                f"检测到主备互为主从闭环: {' -> '.join(cycle_nodes)} -> {master_ip}"
            )
            return resolve_cycle_master(cycle_nodes)

        return find_master_recursive(master_ip, depth + 1, current_path)
    finally:
        conn.close()


def find_slaves_recursive(
    ip: str, depth: int = 0, visited: Optional[Set[str]] = None
) -> Set[str]:
    """递归向下查找所有备库。

    Args:
        ip: 当前探测节点 IP。
        depth: 当前递归深度。
        visited: 已访问过的节点集合。

    Returns:
        Set[str]: 发现的所有备库 IP 集合。
    """
    if visited is None:
        visited = set()

    if depth >= MAX_RECURSION_DEPTH:
        logging.warning(f"达到最大递归深度 {MAX_RECURSION_DEPTH}，停止查找")
        return visited

    if ip in visited:
        return visited

    visited.add(ip)
    conn = connect_mysql(ip)
    if not conn:
        return visited

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT USER, HOST FROM information_schema.processlist 
                WHERE COMMAND = 'Binlog Dump' OR COMMAND = 'Binlog Dump GTID'
                """
            )
            processes = cursor.fetchall()

            for process in processes:
                host = process["HOST"]
                if host and ":" in host:
                    slave_ip = host.split(":")[0]
                    if slave_ip not in visited:
                        logging.info(f"从 {ip} 找到备库: {slave_ip}")
                        visited.update(
                            find_slaves_recursive(slave_ip, depth + 1, visited)
                        )

        return visited
    except pymysql.Error as e:
        logging.error(f"查找备库失败: {e}")
        return visited
    finally:
        conn.close()


def get_relay_master_log_file(conn: pymysql.Connection) -> Optional[str]:
    """获取备库中继回放日志文件（Relay_Master_Log_File）。

    Args:
        conn: 备库 MySQL 连接对象。

    Returns:
        Optional[str]: 日志文件名称，若获取失败或 I/O 线程未运行则返回 None。
    """
    result = query_replica_status(conn)
    if not result:
        logging.error("无法获取备库复制状态信息")
        return None

    io_running = result.get("Replica_IO_Running") or result.get("Slave_IO_Running")
    if io_running != "Yes":
        logging.error(f"备库复制 I/O 线程未运行 (Slave_IO_Running: {io_running})")
        return None

    relay_file = result.get("Relay_Source_Log_File") or result.get("Relay_Master_Log_File")
    if not relay_file:
        logging.error("备库复制状态中未包含 Relay_Master_Log_File 信息")
        return None

    return relay_file


def decrement_binlog_file(binlog_file: str) -> Optional[str]:
    """对二进制日志文件减 1 计算安全清理位点。

    Args:
        binlog_file: 原始二进制日志文件名（期望格式 mysql-bin.XXXXXX）。

    Returns:
        Optional[str]: 减 1 后的文件名，序号 <= 1 或格式无效返回 None。
    """
    match = re.match(r"^mysql-bin\.(\d{6})$", binlog_file)
    if not match:
        logging.error(f"无效的二进制日志文件格式: {binlog_file}")
        return None

    number = int(match.group(1))
    if number <= 1:
        logging.info(f"日志序号已 <= 1，无更早的历史日志需要清理: {binlog_file}")
        return None

    new_binlog_file = f"mysql-bin.{number - 1:06d}"
    logging.info(f"计算安全清理位点: {binlog_file} -> {new_binlog_file}")
    return new_binlog_file


def purge_binary_logs(
    master_conn: pymysql.Connection, binlog_file: str, dry_run: bool = False
) -> bool:
    """在主库执行 PURGE BINARY LOGS 命令。

    Args:
        master_conn: 主库 MySQL 连接对象。
        binlog_file: 目标位点文件名。
        dry_run: 是否为演练模式，True 时仅打印不执行。

    Returns:
        bool: 成功返回 True，失败返回 False。
    """
    purge_cmd = f"PURGE BINARY LOGS TO '{binlog_file}'"
    if dry_run:
        logging.info(f"[DRY-RUN 演练模式] 模拟在主库执行: {purge_cmd}")
        return True

    try:
        with master_conn.cursor() as cursor:
            logging.info(f"执行命令: {purge_cmd}")
            cursor.execute(purge_cmd)
            logging.info("二进制日志清理成功")
            return True
    except pymysql.Error as e:
        logging.error(f"执行 purge binary logs 失败: {e}")
        return False


def main() -> None:
    """主程序入口：解析参数、拓扑探测、位点汇聚计算并安全清理。"""
    parser = argparse.ArgumentParser(description="MySQL二进制日志清理工具")
    parser.add_argument("ip", help="MySQL服务器IP地址（支持集群内任意已知节点）")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="演练模式，仅计算目标清理位点并打印 SQL，不实际执行清理",
    )
    args = parser.parse_args()

    logging.info("=" * 50)
    logging.info(f"开始处理起始 IP: {args.ip} (演练模式: {args.dry_run})")

    # 步骤 1: 递归向上查找主库（支持双主成环破环判定）
    master_ip = find_master_recursive(args.ip)
    if not master_ip:
        logging.error("无法找到有效主库，终止操作")
        return

    logging.info(f"最终主库IP: {master_ip}")

    # 步骤 2: 连接到主库并检查是否可写
    master_conn = connect_mysql(master_ip)
    if not master_conn:
        return

    try:
        if not is_master_writable(master_conn):
            logging.error(f"主库 {master_ip} 不可写（read_only != 0）")
            return

        # 步骤 3: 递归向下查找所有备库
        all_slaves = find_slaves_recursive(master_ip)
        slave_ips = [ip for ip in all_slaves if ip != master_ip]
        logging.info(f"找到备库列表: {slave_ips}")

        if not slave_ips:
            logging.warning("未检测到任何下属备库，为防止误删未备份日志，跳过清理")
            return

        # 步骤 4: 收集各备库的 Relay_Master_Log_File（Fail-Fast 严格断言）
        relay_files = {}
        for slave_ip in slave_ips:
            slave_conn = connect_mysql(slave_ip)
            if not slave_conn:
                logging.error(
                    f"备库 {slave_ip} 无法连接，触发 Fail-Fast 快速失败中止清理"
                )
                return

            try:
                relay_file = get_relay_master_log_file(slave_conn)
                if not relay_file:
                    logging.error(
                        f"备库 {slave_ip} 未能获取有效 Relay_Master_Log_File，触发 Fail-Fast 快速失败中止清理"
                    )
                    return
                if not re.match(r"^mysql-bin\.\d{6}$", relay_file):
                    logging.error(
                        f"备库 {slave_ip} 位点格式不合规 ({relay_file})，触发 Fail-Fast 快速失败中止清理"
                    )
                    return
                relay_files[slave_ip] = relay_file
                logging.info(f"备库 {slave_ip} 当前回放位点: {relay_file}")
            finally:
                slave_conn.close()

        # 步骤 5: 提取全局最小位点并计算安全清理目标位点
        min_relay_file = min(relay_files.values())
        lagging_slaves = [
            ip for ip, rf in relay_files.items() if rf == min_relay_file
        ]
        logging.info(
            f"全部备库最小回放位点: {min_relay_file} (对应滞后备库: {lagging_slaves})"
        )

        target_file = decrement_binlog_file(min_relay_file)
        if not target_file:
            return

        # 步骤 6: 在主库统一执行一次清理
        purge_binary_logs(master_conn, target_file, dry_run=args.dry_run)
    finally:
        master_conn.close()

    logging.info("处理完成")
    logging.info("=" * 50)


if __name__ == "__main__":
    main()

