#!/usr/bin/env python3
import os
import re

# 设置环境变量使用纯Python解析protocol buffers（速度较慢但兼容性好）
os.environ["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"

import argparse
import pymysql
import etcd3
import json
import sys
from typing import Set, Dict, Any
import jinja2

# Database configuration
MYSQL_CONFIG = {
    "host": "192.168.0.10",
    "user": "",
    "password": "",
    "database": "",
}

idc_map = {
    "p1": "192.168.250",
    "p2": "192.168.251",
    "p3": "192.168.252",
}

# etcd configuration
ETCD_HOST = "127.0.0.1"
ETCD_PORT = 2379


def get_idc(ip: str) -> str:
    return "p1"


def match_mdbp(cluster_name: str) -> bool:
    """排除mdbp集群"""
    # 在任何位置匹配mdbp后面跟着数字的模式（如mdbp01, mdbp02, mdbp0202等）
    pattern = r"mdbp\d+"
    res = re.search(pattern, cluster_name) is not None
    return res


def get_vip_from_ip(ip: str) -> str:
    """获取vip"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        query = """
SELECT 
    mci.ip AS instance_ip,
    mci.cluster_name,
    mc.cluster_vip_port
FROM 
    mysql_cluster_instance mci
JOIN 
    mysql_cluster mc ON mci.cluster_name = mc.cluster_name
WHERE 
    mci.ip = %s
        """
        cursor.execute(query, (ip,))
        result = cursor.fetchone()

        if not result:
            raise ValueError(f"No cluster found for IP: {ip}")
        if match_mdbp(result["cluster_name"]):
            raise ValueError(f"mdbp集群不需要重新部署备份: {result['cluster_name']}")

        vip = result["cluster_vip_port"].split("_")[0]
        cursor.close()
        conn.close()

        return vip
    except Exception as err:
        print(f"获取vip出错: {err}")
        sys.exit(1)


def get_cluster_ips(ip: str) -> Set[str]:
    """获取集群中所有ip"""
    try:
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(pymysql.cursors.DictCursor)

        query = """
SELECT 
    ip, 
    instance_role,
    instance_read_only
FROM 
    mysql_cluster_instance
WHERE 
    cluster_name = (SELECT cluster_name FROM mysql_cluster_instance WHERE ip = %s);
"""
        cursor.execute(query, (ip,))
        results = cursor.fetchall()

        ips = set()
        for row in results:
            ips.add(row["ip"])
            print(
                f"MySQL IP: {row['ip']}, Role: {row['instance_role']}, Read Only: {row['instance_read_only']}"
            )

        cursor.close()
        conn.close()

        return ips
    except pymysql.Error as err:
        print(f"MySQL Error: {err}")
        sys.exit(1)


def get_etcd_ha(vip: str) -> Set[str]:
    "获取etcd中的ha信息"
    ip_prefix2 = ".".join(vip.split(".")[:1])
    # 通过vip获取仲裁前缀ip段后查询该仲裁下的所有ha信息
    arbit_prefix = idc_map.get(get_idc(vip))
    etcd_path = f"/db/ha/arbit/{arbit_prefix}"
    try:
        # get all etcd keys and values under the specified path recursively
        etcd = etcd3.client(host=ETCD_HOST, port=ETCD_PORT)
        for value, metadata in etcd.get_prefix(etcd_path):
            if value:
                try:
                    data = json.loads(value.decode("utf-8"))
                    etcd_vip = data.get("vip")
                    if vip == etcd_vip:
                        return data
                except json.JSONDecodeError as err:
                    print(
                        f"JSON Parse Error for key {metadata.key.decode('utf-8')}: {err}"
                    )
        return None

    except etcd3.exceptions.Etcd3Exception as err:
        print(f"etcd Error: {err}")
        sys.exit(1)
    except json.JSONDecodeError as err:
        print(f"JSON Parse Error: {err}")
        sys.exit(1)

def gen_template(db_ips, ha_ips):
    ip_new = list(db_ips - ha_ips)
    template_str = """
[remove_backup]
{%- for ip in db_ips %}
{{ ip }}
{%- endfor %}

[new_backup]
{{ ip_new[0] }}
"""
    
    template = jinja2.Template(template_str)
    rendered = template.render(db_ips=db_ips, ip_new=ip_new)
    print(rendered)
    #将rendenered 写入到文件/tmp/xx中,如果文件不存在则创建,存在则覆盖
    file_path='/tmp/xx'
    try:
        os.remove(file_path)
    except FileNotFoundError:
        pass  # 文件不存在时忽略错误
    with open(file_path, "w") as f:
        f.write(rendered)


def main():
    parser = argparse.ArgumentParser(
        description="生成更新备份列表"
    )
    parser.add_argument("ip", help="要调整集群的IP地址")
    args = parser.parse_args()

    print(f"查询IP: {args.ip}")

    # 获取集群vip
    vip = get_vip_from_ip(args.ip)
    print(f"VIP: {vip}")

    # 获取该集群下ip列表
    mysql_ips = get_cluster_ips(args.ip)
    print(f"IP列表: {mysql_ips}")

    # Step 3: Get IPs from etcd
    etcd_ips = get_etcd_ha(vip)
    print(f"etcd IP列表: {etcd_ips}")

    # Step 4: Compute difference
    diff_ips = mysql_ips - etcd_ips
    print(f"差异IP（在MySQL中但不在etc中）: {diff_ips}")

    if len(diff_ips) != 1:
        print("集群和etcd的IP差集不为1")
    gen_template(cluster_ips, diff_ips)
    # main()
    # get_vip_from_ip("192.168.0.1")
    cluster_ips = get_cluster_ips("192.168.0.1")
    vip = get_vip_from_ip("192.168.0.1")
    ha = get_etcd_ha(vip)
    ha_ips = {ha["master"], ha["slave"]} if ha else set()
    gen_template(cluster_ips, ha_ips)
    # print(cluster_ips)
    # print(ha_ips)
    # print(cluster_ips - ha_ips)