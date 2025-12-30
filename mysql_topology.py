import pymysql
import sys
import os
from dotenv import load_dotenv

# 配置区：从环境变量加载
load_dotenv()
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = int(os.getenv('DB_PORT', '3306'))

class MySQLTopology:
    def __init__(self, user, password, port):
        self.user = user
        self.password = password
        self.port = port
        self.nodes = {}   # 存储节点属性 {ip: {server_id, read_only, ...}}
        self.edges = set() # 存储拓扑关系 (from_ip, to_ip)
        self.visited = set()

    def get_conn(self, host):
        return pymysql.connect(
            host=host, user=self.user, password=self.password, 
            port=self.port, connect_timeout=3, cursorclass=pymysql.cursors.DictCursor
        )

    def probe(self, ip):
        if ip in self.visited:
            return
        self.visited.add(ip)
        
        print(f"[*] 正在探测节点: {ip}")
        try:
            conn = self.get_conn(ip)
            with conn.cursor() as cursor:
                # 1. 获取基本元数据
                cursor.execute("SELECT @@server_id as sid, @@read_only as ro, @@hostname as hn")
                meta = cursor.fetchone()
                self.nodes[ip] = meta

                # 2. 向上找主库 (Source)
                # 兼容 8.0.22+ 和旧版本
                try:
                    cursor.execute("SHOW REPLICA STATUS")
                    slave_info = cursor.fetchone()
                except:
                    cursor.execute("SHOW SLAVE STATUS")
                    slave_info = cursor.fetchone()

                if slave_info:
                    # 获取主库 IP。注意：这里依赖 report_host 或正确的解析
                    master_host = slave_info.get('Source_Host') or slave_info.get('Master_Host')
                    if master_host and master_host not in ['127.0.0.1', 'localhost']:
                        self.edges.add((master_host, ip))
                        self.probe(master_host)

                # 3. 向下找从库 (Replicas)
                try:
                    cursor.execute("SHOW REPLICAS")
                    slaves = cursor.fetchall()
                except:
                    cursor.execute("SHOW SLAVE HOSTS")
                    slaves = cursor.fetchall()

                for s in slaves:
                    s_ip = s.get('Host')
                    if s_ip:
                        self.edges.add((ip, s_ip))
                        self.probe(s_ip)

            conn.close()
        except Exception as e:
            print(f"[!] 无法访问 {ip}: {e}")

    def render(self):
        print("\n" + "="*50)
        print("🔎 MySQL 集群拓扑识别结果")
        print("="*50)

        # 识别双主关系
        masters_dual = []
        normal_edges = []
        
        processed_edges = set()
        for u, v in self.edges:
            if (v, u) in self.edges:
                pair = tuple(sorted((u, v)))
                if pair not in masters_dual:
                    masters_dual.append(pair)
            else:
                normal_edges.append((u, v))

        if masters_dual:
            print("\n[双主架构 (Master-Master)]")
            for m1, m2 in masters_dual:
                print(f"  {m1} <====> {m2}")

        print("\n[主从复制链 (Replication Chains)]")
        # 简单输出拓扑图
        for u, v in self.edges:
            # 如果是双主中的一条边，跳过普通显示
            is_dual = any(u in p and v in p for p in masters_dual)
            arrow = "<==>" if is_dual else "---->"
            ro_status = "(read-only)" if self.nodes.get(v, {}).get('ro') == 1 else "(writable)"
            print(f"  {u} {arrow} {v} {ro_status}")

        print("\n[节点详情]")
        for ip, info in self.nodes.items():
            role = "Slave" if info['ro'] else "Master/Candidate"
            print(f"  - {ip:15} | ID: {info['sid']:<5} | Hostname: {info['hn']:<15} | Role: {role}")

def main():
    if len(sys.argv) < 2:
        print("使用方法: python3 topology_scan.py <集群内任意IP>")
        return
    
    start_ip = sys.argv[1]
    scanner = MySQLTopology(DB_USER, DB_PASSWORD, DB_PORT)
    scanner.probe(start_ip)
    scanner.render()

if __name__ == "__main__":
    main()