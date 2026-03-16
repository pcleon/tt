#!/usr/bin/env python3
"""GTID一致性检查 + 重置并恢复复制

用法示例：
  python3 gtid_reset.py --hosts 192.168.1.10,192.168.1.11 --user root --password 123456 \
      --replica-user repl --replica-password replpass
"""

import argparse
import sys
import pymysql
from pymysql.err import OperationalError


def parse_args():
    p = argparse.ArgumentParser(description="检查 GTID 是否一致然后重置并恢复复制")
    p.add_argument("--hosts", required=True,
                   help="逗号分隔MySQL主机IP, 例如 10.0.0.1,10.0.0.2")
    p.add_argument("--user", required=True, help="MySQL 登录用户")
    p.add_argument("--password", required=True, help="MySQL 登录密码")
    p.add_argument("--port", type=int, default=3306)
    p.add_argument("--replica-user", required=True, help="复制账号")
    p.add_argument("--replica-password", required=True, help="复制账号密码")
    p.add_argument("--dry-run", action="store_true", help="只检查一致性，不执行重置")
    return p.parse_args()


def connect(host, port, user, password):
    return pymysql.connect(host=host,
                           port=port,
                           user=user,
                           password=password,
                           charset="utf8mb4",
                           cursorclass=pymysql.cursors.DictCursor,
                           autocommit=True,
                           connect_timeout=10)


def get_gtid_executed(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT @@global.gtid_executed AS gtid")
        row = cur.fetchone()
    return (row["gtid"] or "").strip()

def run_sql(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)


def get_read_only_status(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT @@global.read_only AS read_only, @@global.super_read_only AS super_read_only")
        row = cur.fetchone()
    return {
        "read_only": bool(row["read_only"]),
    }


def set_read_only(conn, read_only=True)
    run_sql(conn, "SET GLOBAL read_only = %s" % (1 if read_only else 0))


def get_slave_topology(conn, host):
    with conn.cursor() as cur:
        cur.execute("SHOW SLAVE STATUS")
        status = cur.fetchone()
    if not status:
        return None
    master_host = status.get("Master_Host")
    master_port = status.get("Master_Port")
    if not master_host:
        return None
    return {
        "master_host": master_host,
        "master_port": int(master_port) if master_port else 3306,
    }


def reset_instance(conn, host):
    print(f"[{host}] 开始重置: STOP SLAVE, RESET SLAVE ALL, RESET MASTER")
    run_sql(conn, "STOP SLAVE;")
    run_sql(conn, "RESET SLAVE ALL;")
    run_sql(conn, "RESET MASTER;")
    try:
        run_sql(conn, "SET GLOBAL gtid_purged='';")
    except Exception as e:
        print(f"[{host}] 设置 gtid_purged='' 失败（可能已有 GTID）: {e}")
    print(f"[{host}] 重置完成")


def restore_replication(slave_conn, slave_host, topo, repl_user, repl_password):
    if not topo:
        print(f"[{slave_host}] 无复制配置，跳过恢复复制")
        return
    master_host = topo["master_host"]
    master_port = topo["master_port"]
    print(f"[{slave_host}] 恢复复制到主节点 {master_host}:{master_port}")
    run_sql(slave_conn, "STOP SLAVE;")
    run_sql(slave_conn, "RESET SLAVE ALL;")
    sql = (
        "CHANGE MASTER TO MASTER_HOST=%s, MASTER_PORT=%s, MASTER_USER=%s, MASTER_PASSWORD=%s, "
        "MASTER_AUTO_POSITION=1, GET_MASTER_PUBLIC_KEY=1;"
    )
    with slave_conn.cursor() as cur:
        cur.execute(sql, (master_host, master_port, repl_user, repl_password))
    run_sql(slave_conn, "START SLAVE;")
    with slave_conn.cursor() as cur:
        cur.execute("SHOW SLAVE STATUS")
        status = cur.fetchone()
    if not status:
        print(f"[{slave_host}] SHOW SLAVE STATUS 返回空，可能恢复失败")
        return
    io_running = status.get("Slave_IO_Running")
    sql_running = status.get("Slave_SQL_Running")
    last_error = status.get("Last_SQL_Error") or status.get("Last_IO_Error")
    if io_running == "Yes" and sql_running == "Yes":
        print(f"[{slave_host}] 复制恢复成功")
    else:
        print(f"[{slave_host}] 复制恢复失败: Slave_IO_Running={io_running}, Slave_SQL_Running={sql_running}, 错误={last_error}")


def main():
    args = parse_args()
    hosts = [h.strip() for h in args.hosts.split(",") if h.strip()]
    if len(hosts) < 1:
        print("请至少提供一个 MySQL IP")
        sys.exit(1)

    print("开始检查 GTID 一致性...")
    gtid_map = {}
    conns = {}
    topology = {}
    try:
        for host in hosts:
            try:
                conn = connect(host, args.port, args.user, args.password)
                conns[host] = conn
            except OperationalError as e:
                print(f"[{host}] 连接失败: {e}")
                sys.exit(1)
            gtid_map[host] = get_gtid_executed(conn)
            print(f"[{host}] GTID_EXECUTED={gtid_map[host]}")

            topo = get_slave_topology(conn, host)
            if topo:
                topology[host] = topo
                print(f"[{host}] 当前复制源 {topo['master_host']}:{topo['master_port']}")
            else:
                print(f"[{host}] 当前未配置从库，不恢复上游复制")

        values = set(gtid_map.values())
        if len(values) != 1:
            print("GTID 不一致，取消后续重置。每个实例的 gtid_executed 如下：")
            for host, gtid in gtid_map.items():
                print(f"  {host}: {gtid}")
            sys.exit(1)

        print("所有实例 GTID 一致。")

        # 操作前先设置只读，避免写入
        print("操作前先设置只读模式，确保没有新的写入...")
        original_ro = {}
        for host, conn in conns.items():
            original_ro[host] = get_read_only_status(conn)
            if not original_ro[host]["read_only"] or not original_ro[host]["super_read_only"]:
                print(f"[{host}] 设置 read_only=ON, super_read_only=ON")
                set_read_only(conn, True, True)
            else:
                print(f"[{host}] 已是只读模式")

        if args.dry_run:
            print("dry-run 模式，仅检查一致性通过，不执行重置。")
            # dry-run 时恢复原只读设置
            for host, conn in conns.items():
                ro = original_ro[host]
                set_read_only(conn, ro["read_only"], ro["super_read_only"])
            print("已恢复原只读设置")
            return

        print("开始重置所有实例并清空 GTID...")
        for host in hosts:
            reset_instance(conns[host], host)

        print("重置完成，开始恢复原有复制拓扑...")
        for host in hosts:
            restore_replication(conns[host], host, topology.get(host),
                                args.replica_user, args.replica_password)

        print("全部操作完成。请手动确认每个从库 SHOW SLAVE STATUS 是否 IO/SQL 都是 Yes。")

        # 恢复原来只读状态
        print("恢复所有实例原始只读设置...")
        for host, conn in conns.items():
            ro = original_ro.get(host)
            if ro:
                set_read_only(conn, ro["read_only"], ro["super_read_only"])
                print(f"[{host}] 恢复 read_only={ro['read_only']}, super_read_only={ro['super_read_only']}")

    finally:
        for c in conns.values():
            c.close()


if __name__ == "__main__":
    main()
