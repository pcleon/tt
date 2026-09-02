#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从源文件生成数据库复制拓扑配置（rep_master_ip）的脚本.

支持可选表头模式与位置模式：
1. 带表头示例（角色支持 M, S, TS, LS, OS，以及旧版 m, s, l, t, y）：
   cluster M S TS LS OS
   db1 127.0.0.1 127.0.0.2 127.0.0.3 - -
   db2 128.0.0.1 128.0.0.2 - - -

2. 无表头示例（按位置依次对应 db 名称及 M, S, TS, LS, OS）：
   db1 127.0.0.1 127.0.0.2 127.0.0.3
   db2 128.0.0.1 128.0.0.2

输出格式示例：
#db1
127.0.0.1
127.0.0.2 rep_master_ip=127.0.0.1
127.0.0.3 rep_master_ip=127.0.0.2

#db2
128.0.0.1
128.0.0.2 rep_master_ip=128.0.0.1
"""

import argparse
from pathlib import Path
import sys
from typing import List, Optional, Tuple

# 定义标准角色列名及其顺序（M, S, TS, LS, OS）
ROLE_ORDER: List[str] = ["m", "s", "ts", "ls", "os"]
# 兼容旧版角色列名（m, s, l, t, y）
LEGACY_ROLE_ORDER: List[str] = ["m", "s", "l", "t", "y"]


def parse_header(parts: List[str]) -> Optional[Tuple[int, List[int]]]:
    """尝试解析首行表头，提取集群列索引和角色列索引列表.

    Args:
        parts: 首行按空白切分后的字段列表.

    Returns:
        若成功识别为表头，返回 (db_idx, role_indices)；否则返回 None.
    """
    cleaned = [p.strip().lower() for p in parts]
    if not cleaned:
        return None

    # 判断是否包含表头关键词
    has_cluster_kw = any(k in cleaned for k in ("cluster", "db", "instance"))
    has_new_role = any(r in cleaned for r in ("m", "s", "ts", "ls", "os"))
    has_legacy_role = any(r in cleaned for r in ("l", "t", "y"))

    if not (has_cluster_kw or has_new_role or has_legacy_role):
        return None

    # 确定集群名所在列
    db_idx = 0
    for kw in ("cluster", "db", "instance"):
        if kw in cleaned:
            db_idx = cleaned.index(kw)
            break

    # 确定角色列顺序
    header_map = {col_name: idx for idx, col_name in enumerate(cleaned) if idx != db_idx}

    if any(k in header_map for k in ("ts", "ls", "os")):
        role_indices = [header_map[r] for r in ROLE_ORDER if r in header_map]
    elif any(k in header_map for k in ("l", "t", "y")):
        role_indices = [header_map[r] for r in LEGACY_ROLE_ORDER if r in header_map]
    elif any(k in header_map for k in ("m", "s")):
        role_indices = [header_map[r] for r in ("m", "s") if r in header_map]
    else:
        role_indices = [idx for idx in range(len(cleaned)) if idx != db_idx]

    return db_idx, role_indices


def process_lines(lines: List[str]) -> List[str]:
    """从输入行列表解析配置，支持可选表头或纯位置格式.

    按 M -> S -> TS -> LS -> OS 顺序线性链式级联，前一个有效节点作为 rep_master_ip.

    Args:
        lines: 源文件文本行列表.

    Returns:
        生成的配置文本行列表.
    """
    out_lines: List[str] = []
    header_config: Optional[Tuple[int, List[int]]] = None
    first_non_empty = True

    for line in lines:
        s = line.strip()
        if not s:
            continue

        # 检查是否为注释行
        is_comment = s.startswith("#")
        content = s.lstrip("#").strip() if is_comment else s
        parts = content.split()
        if not parts:
            continue

        # 若是第一条有效内容行，尝试识别表头
        if first_non_empty:
            first_non_empty = False
            parsed_header = parse_header(parts)
            if parsed_header is not None:
                header_config = parsed_header
                continue  # 表头行不作为数据行输出

        # 如果此行是普通注释（非表头），直接跳过
        if is_comment:
            continue

        if header_config is not None:
            db_idx, role_indices = header_config
            if db_idx >= len(parts):
                continue
            db = parts[db_idx]
            ips: List[str] = []
            for idx in role_indices:
                if idx < len(parts):
                    val = parts[idx].strip()
                    if val and val not in ("-", "none", "null"):
                        ips.append(val)
        else:
            # 无表头纯位置兼容模式：parts[0] 为 db 名称，后续为各节点 IP（M, S, TS, LS, OS）
            db = parts[0]
            ips = [p for p in parts[1:] if p and p not in ("-", "none", "null")]

        out_lines.append(f"#{db}")
        if not ips:
            continue

        for i, ip in enumerate(ips):
            if i == 0:
                out_lines.append(ip)
            else:
                out_lines.append(f"{ip} rep_master_ip={ips[i - 1]}")

    return out_lines


def main() -> None:
    """命令行入口函数，负责解析参数并执行生成流程."""
    parser = argparse.ArgumentParser(description="从源文件生成 rep_master_ip 配置")
    parser.add_argument("-i", "--input", default="source.txt", help="源文件路径，默认 source.txt")
    parser.add_argument("-o", "--output", help="输出文件路径；若不指定则写到标准输出")
    args = parser.parse_args()

    src = Path(args.input)
    if not src.exists():
        print(f"输入文件不存在: {src}", file=sys.stderr)
        sys.exit(2)

    lines = src.read_text(encoding="utf-8").splitlines()
    out = process_lines(lines)

    output_text = "\n".join(out) + ("\n" if out else "")
    if args.output:
        Path(args.output).write_text(output_text, encoding="utf-8")
    else:
        sys.stdout.write(output_text)


if __name__ == "__main__":
    main()
