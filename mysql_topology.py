#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import pymysql
from dotenv import load_dotenv
from collections import defaultdict

# 加载环境变量
load_dotenv()

# 从环境变量获取MySQL连接信息
MYSQL_USER = os.getenv('MYSQL_USER', 'root')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', '3306'))

class MySQLTopologyAnalyzer:
    def __init__(self):
        self.connections = {}  # 缓存连接
        self.topology = {}     # 存储拓扑结构
        self.visited = set()   # 记录已访问节点
        self.read_only_status = {}  # 存储read_only状态

    def get_connection(self, host, port=MYSQL_PORT):
        """获取MySQL连接，带缓存"""
        key = f"{host}:{port}"
        if key not in self.connections:
            try:
                connection = pymysql.connect(
                    host=host,
                    port=port,
                    user=MYSQL_USER,
                    password=MYSQL_PASSWORD,
                    connect_timeout=5,
                    read_timeout=10,
                    charset='utf8mb4'
                )
                self.connections[key] = connection
            except Exception as e:
                print(f"无法连接到 {host}:{port} - {str(e)}")
                return None
        return self.connections[key]

    def get_master_info(self, host, port=MYSQL_PORT):
        """获取节点的主库信息"""
        connection = self.get_connection(host, port)
        if not connection:
            return None

        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW SLAVE STATUS")
                result = cursor.fetchone()
                if result:
                    # 返回Master信息
                    return {
                        'master_host': result[1],  # Master_Host
                        'master_port': result[3] or MYSQL_PORT,  # Master_Port
                    }
        except Exception as e:
            print(f"查询 {host}:{port} 的主库信息失败: {str(e)}")
        return None

    def get_slave_hosts(self, host, port=MYSQL_PORT):
        """获取节点的所有从库信息"""
        connection = self.get_connection(host, port)
        if not connection:
            return []

        slaves = []
        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW SLAVE HOSTS")
                results = cursor.fetchall()
                for row in results:
                    # SHOW SLAVE HOSTS 返回的信息可能因MySQL版本而异
                    # 通常包含 Server_id, Host, Port, etc.
                    if len(row) >= 3:
                        slave_info = {
                            'server_id': row[0],
                            'host': row[1],
                            'port': row[2] or MYSQL_PORT
                        }
                        slaves.append(slave_info)
        except Exception as e:
            print(f"查询 {host}:{port} 的从库信息失败: {str(e)}")
        
        return slaves

    def get_read_only_status(self, host, port=MYSQL_PORT):
        """获取节点的read_only状态"""
        connection = self.get_connection(host, port)
        if not connection:
            return None

        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT @@read_only, @@super_read_only")
                result = cursor.fetchone()
                if result:
                    return {
                        'read_only': bool(result[0]),
                        'super_read_only': bool(result[1])
                    }
        except Exception as e:
            print(f"查询 {host}:{port} 的read_only状态失败: {str(e)}")
        return None

    def find_top_master(self, host, port=MYSQL_PORT):
        """向上查找顶层主库，处理双主情况"""
        current_host = host
        current_port = port
        path = []  # 记录查找路径
        
        while True:
            # 检查是否已访问过（防止环路）
            if (current_host, current_port) in path:
                # 检测到环路，返回环路中的第一个节点作为顶层
                print(f"检测到复制环路，将 {path[0][0]}:{path[0][1]} 作为顶层节点")
                return path[0]
            
            path.append((current_host, current_port))
            
            # 获取当前节点的主库信息
            master_info = self.get_master_info(current_host, current_port)
            if not master_info:
                # 当前节点没有主库，即为顶层节点
                print(f"找到顶层节点: {current_host}:{current_port}")
                return (current_host, current_port)
            
            master_host = master_info['master_host']
            master_port = master_info['master_port']
            
            # 检查是否存在双主情况
            # 即当前节点是其主库的主库
            master_master_info = self.get_master_info(master_host, master_port)
            if master_master_info:
                if (master_master_info['master_host'] == current_host and 
                    master_master_info['master_port'] == current_port):
                    # 检测到双主结构
                    print(f"检测到双主结构: {current_host}:{current_port} <-> {master_host}:{master_port}")
                    # 返回其中一个作为顶层（选择IP地址较小的）
                    if current_host <= master_host:
                        return (current_host, current_port)
                    else:
                        return (master_host, master_port)
            
            # 继续向上查找
            current_host = master_host
            current_port = master_port

    def build_topology_downward(self, host, port=MYSQL_PORT):
        """从指定节点向下构建复制拓扑"""
        # 避免重复访问
        if (host, port) in self.visited:
            return
        
        self.visited.add((host, port))
        
        # 获取read_only状态
        read_only_info = self.get_read_only_status(host, port)
        if read_only_info:
            self.read_only_status[(host, port)] = read_only_info
        
        # 获取从库信息
        slaves = self.get_slave_hosts(host, port)
        if (host, port) not in self.topology:
            self.topology[(host, port)] = []
        
        # 递归处理所有从库
        for slave in slaves:
            slave_host = slave['host']
            slave_port = slave['port']
            self.topology[(host, port)].append((slave_host, slave_port))
            self.build_topology_downward(slave_host, slave_port)

    def print_topology(self, host, port=MYSQL_PORT, indent=0, is_last=True, prefix=""):
        """以树形结构打印拓扑"""
        # 构建当前行的前缀
        if indent == 0:
            tree_prefix = "└── " if is_last else "├── "
        else:
            tree_prefix = prefix + ("└── " if is_last else "├── ")
        
        # 获取read_only状态
        read_only_info = self.read_only_status.get((host, port), {})
        read_only = read_only_info.get('read_only', False)
        super_read_only = read_only_info.get('super_read_only', False)
        
        # 构建状态字符串
        status_str = ""
        if read_only:
            status_str += " [RO]"
        if super_read_only:
            status_str += " [SRO]"
        
        # 打印当前节点
        print(f"{tree_prefix}{host}:{port}{status_str}")
        
        # 构建子节点前缀
        if indent == 0:
            child_prefix = "    " if is_last else "│   "
        else:
            child_prefix = prefix + ("    " if is_last else "│   ")
        
        # 递归打印子节点
        children = self.topology.get((host, port), [])
        for i, child in enumerate(children):
            is_last_child = (i == len(children) - 1)
            self.print_topology(child[0], child[1], indent + 1, is_last_child, child_prefix)

    def analyze(self, start_host, start_port=MYSQL_PORT):
        """分析复制拓扑"""
        print(f"开始分析 {start_host}:{start_port} 的复制拓扑...")
        
        # 1. 向上查找顶层主库
        top_master = self.find_top_master(start_host, start_port)
        if not top_master:
            print("无法找到顶层主库")
            return
        
        top_host, top_port = top_master
        
        # 2. 从顶层主库向下构建完整拓扑
        print(f"从顶层节点 {top_host}:{top_port} 开始构建拓扑...")
        self.build_topology_downward(top_host, top_port)
        
        # 3. 打印拓扑结构
        print("\nMySQL复制拓扑结构:")
        print("=" * 50)
        self.print_topology(top_host, top_port)
        print("=" * 50)
        
        # 4. 统计信息
        # total_nodes = len(self.read_only_status)
        # read_only_nodes = sum(1 for info in self.read_only_status.values() if info.get('read_only'))
        # super_read_only_nodes = sum(1 for info in self.read_only_status.values() if info.get('super_read_only'))
        
        # print(f"\n统计信息:")
        # print(f"  总节点数: {total_nodes}")
        # print(f"  只读节点数: {read_only_nodes}")
        # print(f"  超级只读节点数: {super_read_only_nodes}")

def main():
    if len(sys.argv) < 2:
        print("用法: python3 mysql_topology.py <IP地址> [端口]")
        print("示例: python3 mysql_topology.py 192.168.1.100")
        print("示例: python3 mysql_topology.py 192.168.1.100 3307")
        sys.exit(1)
    
    host = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else MYSQL_PORT
    
    analyzer = MySQLTopologyAnalyzer()
    analyzer.analyze(host, port)

if __name__ == "__main__":
    main()
