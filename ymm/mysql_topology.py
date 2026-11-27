from collections import defaultdict
import pymysql
import sys
import os
import json
from dotenv import load_dotenv

# 配置区：从环境变量加载
load_dotenv()
DB_USER = os.getenv('DB_USER', 'root')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_PORT = int(os.getenv('DB_PORT', '3306'))


class TermColors:
    """终端颜色配置"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'  # 绿色：可写/主
    WARNING = '\033[93m'  # 黄色：双主/警告
    FAIL = '\033[91m'     # 红色：错误
    CYAN = '\033[96m'     # 青色：只读/从
    GREY = '\033[90m'
    BOLD = '\033[1m'
    ENDC = '\033[0m'

class TopologyScanner:
    def __init__(self, user, password, port):
        self.user = user
        self.password = password
        self.port = port
        self.nodes = {}       # 存储节点元数据
        self.edges = set()    # 存储拓扑关系 (parent, child)
        self.visited = set()  # 防止递归死循环
        self.dual_masters = set() # 存储双主对

    def get_conn(self, host):
        return pymysql.connect(
            host=host, user=self.user, password=self.password, 
            port=self.port, connect_timeout=3, 
            cursorclass=pymysql.cursors.DictCursor
        )

    def scan(self, ip):
        """递归扫描核心逻辑"""
        if ip in self.visited:
            return
        self.visited.add(ip)

        # 简单的进度打印
        sys.stdout.write(f"\rScanning node: {ip} ...\033[K")
        sys.stdout.flush()

        try:
            conn = self.get_conn(ip)
            with conn.cursor() as cursor:
                # 1. 获取节点基础信息
                cursor.execute("SELECT @@server_id as sid, @@read_only as ro, @@hostname as hn, @@version as ver")
                meta = cursor.fetchone()
                # 补充 IP 字段方便后续 JSON 序列化
                meta['ip'] = ip
                self.nodes[ip] = meta

                # 2. 向上探测 (Find Master)
                # 优先尝试 MySQL 8.0.22+ 新语法，失败则回退
                try:
                    cursor.execute("SHOW REPLICA STATUS")
                    m_status = cursor.fetchone()
                except:
                    cursor.execute("SHOW SLAVE STATUS")
                    m_status = cursor.fetchone()
                
                if m_status:
                    m_host = m_status.get('Source_Host') or m_status.get('Master_Host')
                    # 排除本地回环
                    if m_host and m_host not in ['127.0.0.1', 'localhost', '::1']:
                        self.edges.add((m_host, ip))
                        self.scan(m_host)

                # 3. 向下探测 (Find Slaves)
                try:
                    cursor.execute("SHOW REPLICAS")
                    s_hosts = cursor.fetchall()
                except:
                    cursor.execute("SHOW SLAVE HOSTS")
                    s_hosts = cursor.fetchall()

                for s in s_hosts:
                    s_ip = s['Host']
                    self.edges.add((ip, s_ip))
                    self.scan(s_ip)

            conn.close()
        except Exception as e:
            self.nodes[ip] = {'ip': ip, 'error': str(e), 'ro': -1, 'sid': -1}

    def analyze(self):
        """分析拓扑结构，提取双主和树形关系"""
        # 1. 识别双主 (A->B 且 B->A)
        for u, v in self.edges:
            if (v, u) in self.edges:
                pair = tuple(sorted((u, v)))
                self.dual_masters.add(pair)

        # 2. 构建邻接表 (用于树形打印)
        self.tree_map = defaultdict(list)
        self.children_set = set()
        
        for u, v in self.edges:
            # 如果是双主关系，在画树时切断循环，避免死循环打印
            is_dual_link = False
            for dm in self.dual_masters:
                if u in dm and v in dm:
                    is_dual_link = True
            
            if not is_dual_link:
                self.tree_map[u].append(v)
                self.children_set.add(v)

    def _print_node(self, ip):
        """格式化单个节点的显示字符串"""
        info = self.nodes.get(ip, {})
        if 'error' in info:
            return f"{TermColors.FAIL}[X] {ip} (Conn Error){TermColors.ENDC}"

        is_ro = info.get('ro') == 1
        
        # 图标和颜色定义
        if is_ro:
            icon = "🧊" # 冰块表示只读
            color = TermColors.CYAN
            role = "RO"
        else:
            icon = "🔥" # 火焰表示读写
            color = TermColors.OKGREEN
            role = "RW"

        # 检查是否为双主
        dm_flag = ""
        for pair in self.dual_masters:
            if ip in pair:
                icon = "♻️ " # 循环标志
                color = TermColors.WARNING
                dm_flag = f" {TermColors.BOLD}[双主]{TermColors.ENDC}"
                role = "MM"

        return f"{color}{icon} {ip}{TermColors.ENDC} ({role}, id:{info.get('sid')}){dm_flag}"

    def print_tree_recursive(self, root, prefix=""):
        print(f"{prefix}{self._print_node(root)}")
        children = self.tree_map.get(root, [])
        count = len(children)
        for i, child in enumerate(children):
            is_last = (i == count - 1)
            marker = "└── " if is_last else "├── "
            new_prefix = prefix + ("    " if is_last else "│   ")
            sys.stdout.write(prefix + marker)
            self.print_tree_recursive(child, new_prefix)

    def render_terminal(self):
        """打印人类可读的终端图形"""
        self.analyze()
        sys.stdout.write("\r" + " " * 50 + "\r") # 清除进度条
        print(f"\n{TermColors.HEADER}=== MySQL 拓扑结构 ==={TermColors.ENDC}\n")

        # 寻找根节点：不在"孩子集合"中的节点，或者是双主之一
        # 注意：双主中的节点互为父子，如果不处理会被漏掉。
        # 逻辑：先处理双主，再处理剩下的独立树。
        
        processed_roots = set()

        # 1. 优先展示双主架构
        if self.dual_masters:
            print(f"{TermColors.BOLD}>>> 检测到双主 (Master-Master) 架构:{TermColors.ENDC}")
            for m1, m2 in self.dual_masters:
                # 简单的双主并列展示
                print(f" ┌─ {self._print_node(m1)}")
                print(f" ║  (同步复制)")
                print(f" └─ {self._print_node(m2)}")
                
                # 打印挂在 m1 下面的从库
                if self.tree_map[m1]:
                    print(f"    └─ [挂载于 {m1}]")
                    self.print_tree_recursive(m1, prefix="       ") # m1 的子树其实应该跳过自身
                    # 修正：上面的递归会把m1自己打印一遍，这里需要特殊处理
                    # 简单起见，我们直接遍历 m1 的 children 打印
                    children = self.tree_map[m1]
                    for idx, c in enumerate(children):
                         marker = "└── " if idx == len(children)-1 else "├── "
                         sys.stdout.write("       " + marker)
                         self.print_tree_recursive(c, "           ")

                # 打印挂在 m2 下面的从库
                if self.tree_map[m2]:
                    print(f"    └─ [挂载于 {m2}]")
                    children = self.tree_map[m2]
                    for idx, c in enumerate(children):
                         marker = "└── " if idx == len(children)-1 else "├── "
                         sys.stdout.write("       " + marker)
                         self.print_tree_recursive(c, "           ")
                
                print("")
                processed_roots.add(m1)
                processed_roots.add(m2)

        # 2. 展示普通的一主多从 (Standard Master-Slave)
        # 根节点 = 所有节点 - 所有子节点 - 已经处理过的双主节点
        potential_roots = set(self.nodes.keys()) - self.children_set - processed_roots
        
        if potential_roots:
            print(f"{TermColors.BOLD}>>> 普通主从/级联架构:{TermColors.ENDC}")
            for root in potential_roots:
                self.print_tree_recursive(root, prefix=" ")
                print("")

    def generate_json(self):
        """生成程序易读的 JSON 结构"""
        output = {
            "summary": {
                "total_nodes": len(self.nodes),
                "dual_master_detected": len(self.dual_masters) > 0,
                "dual_master_pairs": list(self.dual_masters)
            },
            "topology_edges": list(self.edges),
            "nodes_detail": self.nodes
        }
        return json.dumps(output, indent=2, ensure_ascii=False)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 topology.py <IP_ADDRESS>")
        sys.exit(1)

    target_ip = sys.argv[1]
    scanner = TopologyScanner(DB_USER, DB_PASSWORD, DB_PORT)
    
    # 1. 执行扫描
    scanner.scan(target_ip)
    
    # 2. 终端可视化输出 (Human Readable)
    scanner.render_terminal()

    # 3. JSON 输出 (Machine Readable)
    # 打印分隔符，方便后续程序通过 awk/sed 截取，或者直接重定向
    print(f"{TermColors.GREY}{'-'*20} JSON DATA BEGIN {'-'*20}{TermColors.ENDC}")
    print(scanner.generate_json())
    print(f"{TermColors.GREY}{'-'*20} JSON DATA END {'-'*20}{TermColors.ENDC}")

if __name__ == "__main__":
    main()