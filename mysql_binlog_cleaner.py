#!/usr/bin/env python3
import argparse
import pymysql
import logging
import logging.handlers
import re
from typing import List, Dict, Any, Optional, Set

# 配置日志
LOG_FILE = "/tmp/mysql_binlog_cleaner.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=3,
            encoding='utf-8'
        )
    ]
)

# MySQL配置（硬编码，参考update_backup.py）
MYSQL_CONFIG = {
    "host": "192.168.0.10",
    "user": "",
    "password": "",
    "database": "test",
    "port": 33306,
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# 最大递归深度
MAX_RECURSION_DEPTH = 4

def connect_mysql(host: str) -> Optional[pymysql.Connection]:
    """连接到MySQL服务器"""
    try:
        config = MYSQL_CONFIG.copy()
        config["host"] = host
        conn = pymysql.connect(**config)
        logging.info(f"成功连接到MySQL服务器: {host}")
        return conn
    except pymysql.Error as e:
        logging.error(f"连接MySQL服务器失败 {host}: {e}")
        return None

def is_slave(conn: pymysql.Connection) -> bool:
    """检查当前服务器是否是备库"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW SLAVE STATUS")
            result = cursor.fetchone()
            return result is not None
    except pymysql.Error as e:
        logging.error(f"检查备库状态失败: {e}")
        return False

def get_master_info(conn: pymysql.Connection) -> Optional[Dict[str, Any]]:
    """获取主库信息"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW SLAVE STATUS")
            result = cursor.fetchone()
            if result:
                return {
                    "master_host": result.get("Master_Host"),
                    "master_port": result.get("Master_Port"),
                    "relay_master_log_file": result.get("Relay_Master_Log_File")
                }
            return None
    except pymysql.Error as e:
        logging.error(f"获取主库信息失败: {e}")
        return None

def find_master_recursive(ip: str, depth: int = 0) -> Optional[str]:
    """递归向上查找主库，最大深度4层"""
    if depth >= MAX_RECURSION_DEPTH:
        logging.warning(f"达到最大递归深度 {MAX_RECURSION_DEPTH}，停止查找")
        return None
    
    conn = connect_mysql(ip)
    if not conn:
        return None
    
    try:
        if not is_slave(conn):
            logging.info(f"找到主库: {ip}")
            return ip
        
        master_info = get_master_info(conn)
        if not master_info or not master_info["master_host"]:
            logging.warning(f"无法获取主库信息 from {ip}")
            return None
        
        master_ip = master_info["master_host"]
        logging.info(f"从 {ip} 找到上层主库: {master_ip}")
        return find_master_recursive(master_ip, depth + 1)
    finally:
        conn.close()

def is_master_writable(conn: pymysql.Connection) -> bool:
    """检查主库是否可写"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@read_only AS read_only")
            result = cursor.fetchone()
            return result and result["read_only"] == 0
    except pymysql.Error as e:
        logging.error(f"检查主库可写状态失败: {e}")
        return False

def find_slaves_recursive(ip: str, depth: int = 0, visited: Optional[Set[str]] = None) -> Set[str]:
    """递归向下查找所有备库"""
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
        # 通过processlist查找连接的备库
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT USER, HOST FROM information_schema.processlist 
                WHERE COMMAND = 'Binlog Dump' OR COMMAND = 'Binlog Dump GTID'
            """)
            processes = cursor.fetchall()
            
            for process in processes:
                # 从HOST字段提取IP地址
                host = process["HOST"]
                if host and ":" in host:
                    slave_ip = host.split(":")[0]
                    if slave_ip not in visited:
                        logging.info(f"从 {ip} 找到备库: {slave_ip}")
                        visited.update(find_slaves_recursive(slave_ip, depth + 1, visited))
        
        return visited
    except pymysql.Error as e:
        logging.error(f"查找备库失败: {e}")
        return visited
    finally:
        conn.close()

def get_relay_master_log_file(conn: pymysql.Connection) -> Optional[str]:
    """获取Relay_Master_Log_File"""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW SLAVE STATUS")
            result = cursor.fetchone()
            if result and result.get("Relay_Master_Log_File"):
                return result["Relay_Master_Log_File"]
            return None
    except pymysql.Error as e:
        logging.error(f"获取Relay_Master_Log_File失败: {e}")
        return None

def decrement_binlog_file(binlog_file: str) -> Optional[str]:
    """对二进制日志文件减1"""
    match = re.match(r'^(mysql-bin\.)(\d+)(.*)$', binlog_file)
    if not match:
        logging.error(f"无效的二进制日志文件格式: {binlog_file}")
        return None
    
    prefix = match.group(1)
    number = int(match.group(2))
    suffix = match.group(3)
    
    if number <= 1:
        logging.error(f"无法对二进制日志文件减1: {binlog_file}")
        return None
    
    new_number = number - 1
    new_binlog_file = f"{prefix}{new_number:06d}{suffix}"
    logging.info(f"二进制日志文件减1: {binlog_file} -> {new_binlog_file}")
    return new_binlog_file

def purge_binary_logs(master_conn: pymysql.Connection, binlog_file: str):
    """在主库执行purge binary logs命令"""
    try:
        with master_conn.cursor() as cursor:
            purge_cmd = f"PURGE BINARY LOGS TO '{binlog_file}'"
            logging.info(f"执行命令: {purge_cmd}")
            # cursor.execute(purge_cmd)
            logging.info("二进制日志清理成功")
    except pymysql.Error as e:
        logging.error(f"执行purge binary logs失败: {e}")

def main():
    parser = argparse.ArgumentParser(description="MySQL二进制日志清理工具")
    parser.add_argument("ip", help="MySQL服务器IP地址")
    args = parser.parse_args()
    
    logging.info("="*50)
    logging.info(f"开始处理IP: {args.ip}")
    
    # 步骤1: 递归向上查找主库
    master_ip = find_master_recursive(args.ip)
    if not master_ip:
        logging.error("无法找到主库")
        return
    
    logging.info(f"最终主库IP: {master_ip}")
    
    # 步骤2: 连接到主库并检查是否可写
    master_conn = connect_mysql(master_ip)
    if not master_conn:
        return
    
    try:
        if not is_master_writable(master_conn):
            logging.error(f"主库 {master_ip} 不可写")
            return
        
        # 步骤3: 递归向下查找所有备库
        all_slaves = find_slaves_recursive(master_ip)
        logging.info(f"找到所有备库: {all_slaves}")
        
        # 步骤4: 对每个备库获取Relay_Master_Log_File并处理
        for slave_ip in all_slaves:
            if slave_ip == master_ip:
                continue
                
            slave_conn = connect_mysql(slave_ip)
            if not slave_conn:
                continue
                
            try:
                relay_file = get_relay_master_log_file(slave_conn)
                if not relay_file:
                    logging.warning(f"备库 {slave_ip} 没有Relay_Master_Log_File")
                    continue
                
                target_file = decrement_binlog_file(relay_file)
                if target_file:
                    purge_binary_logs(master_conn, target_file)
            finally:
                slave_conn.close()
                
    finally:
        master_conn.close()
    
    logging.info("处理完成")
    logging.info("="*50)

if __name__ == "__main__":
    main()
