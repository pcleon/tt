import etcd3
import json
import subprocess
from etcd3 import Etcd3Client


# SERVER = '192.168.0.10'
ETCD_SERVER = "127.0.0.1"
ips = etcd3.client(host=ETCD_SERVER, port=2379)


def mysql_ping_ok(ip):
    # 通过mysqladmin ping命令检查mysql的健康状态
    try:
        result = subprocess.run(
            ["mysqladmin", "-h", ip, "ping"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
        )
        if result.returncode == 0:
            return True
        else:
            return False
    except Exception as e:
        print(f"Error checking MySQL health on {ip}: {e}")
        return False


# 定义降级函数
def downgrade(ha_cluster):
    # 检查ha_cluster是json格式
    if isinstance(ha_cluster, bytes):
        try:
            ha_cluster = json.loads(ha_cluster)
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON: {e}")
            return
    master = ha_cluster.get("master")
    slave = ha_cluster.get("slave")
    vip = ha_cluster.get("vip")
    db_instance = ha_cluster.get("db_instance")  # aa_00
    if not mysql_ping_ok(master) or not mysql_ping_ok(slave):
        cmd = f"""ps -ef |grep -P 'manager_master_check.sh|manager_slave_check.sh|master_monit.sh|slave_monit.sh' |grep -v grep |awk '{{print \\$2}}' |xargs kill -9"""
        cmd_master = (
            f"""ssh -o ConnectTimeout=3 -o StrictHostKeyChecking=no {master} '{cmd}'"""
        )
        cmd_slave = (
            f"""ssh -o ConnectTimeout=3 -o StrictHostKeyChecking=no {slave} '{cmd}'"""
        )
        subprocess.run(
            cmd_master, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        print(master)
        subprocess.run(
            cmd_slave, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        print(slave)


key = "/db/ha"  # 替换为你需要查询的键
for cluster, _ in ips.get_prefix(key):
    try:
        downgrade(cluster)
    except Exception as e:
        print(f"Error decoding JSON for key {cluster}: {e}")