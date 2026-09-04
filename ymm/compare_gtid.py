#!/usr/bin/env python3
"""
compare_gtid.py
用法示例:
  python3 compare_gtid.py 192.168.0.10 192.168.0.11
"""

import argparse
import pymysql
import sys
import os
from typing import Dict, List, Tuple

# 使用 python-dotenv 从 .env 文件加载凭据（可选）
try:
    from dotenv import load_dotenv
    load_dotenv(dotenv_path='.env')
except Exception:
    # 如果未安装 python-dotenv 或者没有 .env 文件，则忽略，使用环境变量或脚本默认
    pass

# 默认凭据（从环境读取，或者使用内置默认）
DEFAULT_USER = os.getenv('MYSQL_USER', '')
DEFAULT_PASSWORD = os.getenv('MYSQL_PASSWORD', '')
DEFAULT_PORT = int(os.getenv('MYSQL_PORT', '3306'))

Interval = Tuple[int, int]
GTIDMap = Dict[str, List[Interval]]


def parse_gtid_set(gtid: str) -> GTIDMap:
    """解析 GTID_EXECUTED 字符串为 {uuid: [(start,end), ...], ...} 并合并区间。"""
    result: GTIDMap = {}
    if not gtid:
        return result

    gtid = gtid.strip()
    if gtid == "":
        return result

    # 每个 source 的部分以逗号分隔
    parts = [p.strip() for p in gtid.split(',') if p.strip()]
    for part in parts:
        # 格式: uuid:interval[:interval...]
        segs = part.split(':')
        if len(segs) < 2:
            # 可能是仅 uuid（不太可能），跳过
            continue
        uuid = segs[0]
        intervals_tokens = segs[1:]
        intervals: List[Interval] = []
        for tok in intervals_tokens:
            if tok == '':
                continue
            if '-' in tok:
                a, b = tok.split('-', 1)
                try:
                    start = int(a)
                    end = int(b)
                except ValueError:
                    continue
            else:
                try:
                    start = end = int(tok)
                except ValueError:
                    continue
            intervals.append((start, end))

        if not intervals:
            continue

        # 合并区间
        merged = merge_intervals(intervals)
        if uuid in result:
            # 合并已有与新解析到的区间
            combined = merge_intervals(result[uuid] + merged)
            result[uuid] = combined
        else:
            result[uuid] = merged

    return result


def merge_intervals(intervals: List[Interval]) -> List[Interval]:
    if not intervals:
        return []
    intervals_sorted = sorted(intervals, key=lambda x: x[0])
    merged: List[Interval] = []
    cur_start, cur_end = intervals_sorted[0]
    for s, e in intervals_sorted[1:]:
        if s <= cur_end + 1:
            cur_end = max(cur_end, e)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = s, e
    merged.append((cur_start, cur_end))
    return merged


def intervals_to_str(intervals: List[Interval]) -> str:
    parts = []
    for s, e in intervals:
        if s == e:
            parts.append(str(s))
        else:
            parts.append(f"{s}-{e}")
    return ':'.join(parts)


def gtidmap_to_canonical(gtidmap: GTIDMap) -> str:
    # 按 uuid 排序，格式 uuid:interval:interval,...
    items = []
    for uuid in sorted(gtidmap.keys()):
        parts = intervals_to_str(gtidmap[uuid])
        items.append(f"{uuid}:{parts}")
    return ','.join(items)


def subtract_intervals(a: List[Interval], b: List[Interval]) -> List[Interval]:
    """返回 a - b 的区间列表，假设 a 和 b 都已合并且按起点排序。"""
    if not a:
        return []
    if not b:
        return a.copy()

    res: List[Interval] = []
    bi = 0
    for (as_, ae) in a:
        cur_start = as_
        cur_end = ae
        while bi < len(b) and b[bi][1] < cur_start:
            bi += 1
        ti = bi
        while ti < len(b) and b[ti][0] <= cur_end:
            b_start, b_end = b[ti]
            if b_start <= cur_start <= b_end:
                # cut off left part
                cur_start = b_end + 1
                if cur_start > cur_end:
                    break
            elif cur_start < b_start:
                # take segment before b_start
                seg_end = min(cur_end, b_start - 1)
                if cur_start <= seg_end:
                    res.append((cur_start, seg_end))
                cur_start = b_end + 1
                if cur_start > cur_end:
                    break
            ti += 1
        if cur_start <= cur_end:
            res.append((cur_start, cur_end))
    return res


def compare_gtid_maps(source_map: GTIDMap, replica_map: GTIDMap) -> Tuple[GTIDMap, GTIDMap]:
    """返回 (in_source_not_in_replica, in_replica_not_in_source) 两个 GTIDMap 差异"""
    source_only: GTIDMap = {}
    replica_only: GTIDMap = {}

    all_uuids = set(source_map.keys()) | set(replica_map.keys())
    for uuid in all_uuids:
        a = source_map.get(uuid, [])
        b = replica_map.get(uuid, [])
        if not a and b:
            replica_only[uuid] = b
            continue
        if a and not b:
            source_only[uuid] = a
            continue
        # both exist, subtract
        a_minus_b = subtract_intervals(a, b)
        b_minus_a = subtract_intervals(b, a)
        if a_minus_b:
            source_only[uuid] = a_minus_b
        if b_minus_a:
            replica_only[uuid] = b_minus_a

    return source_only, replica_only


def fetch_gtid(host: str, user: str, password: str, port: int) -> str:
    try:
        conn = pymysql.connect(host=host, user=user, password=password, port=port, connect_timeout=5)
        with conn.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.gtid_executed")
            row = cur.fetchone()
            if not row:
                return ""
            # row may be a tuple
            val = row[0] if isinstance(row, (list, tuple)) else row
            return val if val is not None else ''
    except pymysql.MySQLError as e:
        print(f"连接 {host} 时出错: {e}")
        sys.exit(2)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="比较两个 MySQL 8.0 实例的 GTID_EXECUTED")
    parser.add_argument("source", help="主库 IP")
    parser.add_argument("replica", help="备库 IP")
    parser.add_argument("--user", default=DEFAULT_USER, help="MySQL 用户，默认为空")
    parser.add_argument("--password", default=DEFAULT_PASSWORD, help="MySQL 密码,默认为空")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="MySQL 端口，默认 3306")
    args = parser.parse_args()

    pwd = args.password

    source_gtid = fetch_gtid(args.source, args.user, pwd, args.port)
    replica_gtid = fetch_gtid(args.replica, args.user, pwd, args.port)

    print(f"主库 GTID_EXECUTED:\n{source_gtid}")
    print(f"备库 GTID_EXECUTED:\n{replica_gtid}")

    m_map = parse_gtid_set(source_gtid)
    r_map = parse_gtid_set(replica_gtid)

    # 归一化并比较
    if gtidmap_to_canonical(m_map) == gtidmap_to_canonical(r_map):
        print("GTID 集合相同")
        sys.exit(0)

    source_only, replica_only = compare_gtid_maps(m_map, r_map)

    print(f"\nsource:{args.source} 中存在但 Replica:{args.replica} 中不存在的 GTID:")
    if source_only:
        for uuid in sorted(source_only.keys()):
            print(f"{uuid}:{intervals_to_str(source_only[uuid])}")
    else:
        print("（无）")

    print("\nReplica:{args.replica} 中存在但 source:{args.source} 中不存在的 GTID:")
    if replica_only:
        for uuid in sorted(replica_only.keys()):
            print(f"{uuid}:{intervals_to_str(replica_only[uuid])}")
    else:
        print("（无）")

    sys.exit(0)


if __name__ == '__main__':
    main()
