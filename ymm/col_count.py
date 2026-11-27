# !/usr/bin/env python3
"""
col_count.py

从一个源数据库查询大量 IP（或主机列表），然后逐个连接这些 MySQL 实例，查询每个实例的所有数据库表行数（估算或精确计数），
将表行数大于阈值（默认 5,000,000）的记录汇总输出为 CSV 或控制台。

用法示例:
  python3 col_count.py \
	--source-host 10.0.0.1 --source-user root --source-password pwd \
	--ip-query "SELECT ip FROM servers WHERE type='mysql'" \
	--target-user root --target-password pwd \
	--threshold 5000000 --workers 8 --output large_tables.csv

注意: 需要安装 `pymysql`。推荐在虚拟环境中使用。
"""

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Optional

import pymysql

import os
from dotenv import load_dotenv

# load .env from current working directory (if present)
load_dotenv()

# ==== 固定在脚本中的连接信息与查询（请根据实际环境修改） ====
# 源数据库（用于查询 IP 列表）
# 从环境变量读取，优先使用 .env 中的配置；若未设置则使用以下默认值（请按需修改或在 .env 中配置）
SOURCE_HOST = os.getenv("SOURCE_HOST", "192.168.0.1")
SOURCE_PORT = int(os.getenv("SOURCE_PORT", "3306"))
SOURCE_USER = os.getenv("SOURCE_USER", "root")
SOURCE_PASSWORD = os.getenv("SOURCE_PASSWORD", "pwd")
SOURCE_DB = os.getenv("SOURCE_DB") or None
IP_QUERY = os.getenv("IP_QUERY", "select ip from test.mysql where role = 'L'")

# 目标 MySQL 实例登录信息（如果每个实例用户名/密码相同则使用此处配置）
TARGET_USER = os.getenv("TARGET_USER", "root")
TARGET_PASSWORD = os.getenv("TARGET_PASSWORD", "pwd")
TARGET_PORT = int(os.getenv("TARGET_PORT", "3306"))
# ========================================================


def parse_args():
	p = argparse.ArgumentParser(description="查询多个 MySQL 实例的表行数并汇总超过阈值的表（源/目标连接已固定在脚本内）")
	p.add_argument("--threshold", type=int, default=5_000_000, help="行数阈值，超过则记录（默认 5000000）")
	p.add_argument("--workers", type=int, default=8, help="并发工作线程数（默认 8）")
	p.add_argument("--output", default=None, help="可选：输出 CSV 文件路径；如果不指定则打印到 stdout")
	p.add_argument("--connect-timeout", type=int, default=5, help="连接超时秒数")

	return p.parse_args()


def get_ips_from_source(host: str, port: int, user: str, password: str, db: Optional[str], query: str, timeout: int) -> List[str]:
	conn = pymysql.connect(host=host, port=port, user=user, password=password, database=db, connect_timeout=timeout, cursorclass=pymysql.cursors.DictCursor)
	try:
		with conn.cursor() as cur:
			cur.execute(query)
			rows = cur.fetchall()
		ips = []
		for r in rows:
			# 尝试从任意列拿值（通常是 ip）
			if isinstance(r, dict):
				for v in r.values():
					if v is not None:
						ips.append(str(v))
						break
			else:
				ips.append(str(r[0]))
		return ips
	finally:
		conn.close()


def check_server(ip: str, port: int, user: str, password: str, threshold: int, timeout: int) -> List[Dict]:
	"""连接到单个 MySQL 实例，查询 information_schema.tables 获取每个表的估算行数（仅使用估算值，不执行 COUNT(*)）。

	返回满足阈值的表信息列表。
	"""
	results = []
	try:
		conn = pymysql.connect(host=ip, port=port, user=user, password=password, connect_timeout=timeout, cursorclass=pymysql.cursors.DictCursor)
	except Exception as e:
		return [{"server": ip, "error": f"connect_error: {e}"}]

	try:
		with conn.cursor() as cur:
			cur.execute("SELECT TABLE_SCHEMA, TABLE_NAME, ENGINE, TABLE_ROWS FROM information_schema.tables WHERE TABLE_SCHEMA NOT IN ('mysql','performance_schema','information_schema','sys')")
			tables = cur.fetchall()
			for t in tables:
				est_rows = t.get("TABLE_ROWS") or 0
				if est_rows >= threshold:
					rec = {
						"server": ip,
						"schema": t.get("TABLE_SCHEMA"),
						"table": t.get("TABLE_NAME"),
						"engine": t.get("ENGINE"),
						"estimated_rows": int(est_rows),
						"actual_rows": None,
					}
					# 只保留估算值，不执行实际的 COUNT(*) 查询以避免 IO 开销
					results.append(rec)
		return results
	finally:
		conn.close()


def write_output(records: List[Dict], output_file: Optional[str]):
	fieldnames = ["server", "schema", "table", "engine", "estimated_rows", "actual_rows"]
	if output_file:
		with open(output_file, "w", newline="") as f:
			writer = csv.DictWriter(f, fieldnames=fieldnames)
			writer.writeheader()
			for r in records:
				writer.writerow({k: r.get(k) for k in fieldnames})
	else:
		writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
		writer.writeheader()
		for r in records:
			writer.writerow({k: r.get(k) for k in fieldnames})


def main():
	args = parse_args()

	start = time.time()
	print(f"Fetching IP list from {SOURCE_HOST} using fixed query...")
	ips = get_ips_from_source(SOURCE_HOST, SOURCE_PORT, SOURCE_USER, SOURCE_PASSWORD, SOURCE_DB, IP_QUERY, args.connect_timeout)
	print(f"Found {len(ips)} hosts; checking with {args.workers} workers (threshold={args.threshold})")

	all_records = []
	with ThreadPoolExecutor(max_workers=args.workers) as ex:
		futures = {ex.submit(check_server, ip, TARGET_PORT, TARGET_USER, TARGET_PASSWORD, args.threshold, args.accurate, args.connect_timeout): ip for ip in ips}
		for fut in as_completed(futures):
			ip = futures[fut]
			try:
				res = fut.result()
				if res:
					# res is a list of records or a list with error dict
					for r in res:
						all_records.append(r)
			except Exception as e:
				all_records.append({"server": ip, "error": f"task_error: {e}"})

	# 过滤出有效记录（包含 schema/table）并输出
	filtered = [r for r in all_records if r.get("schema")]
	print(f"Total large tables found: {len(filtered)}; writing output...")
	write_output(filtered, args.output)
	print(f"Done in {time.time()-start:.1f}s")


if __name__ == "__main__":
	main()

