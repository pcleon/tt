#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
单文件脚本：从源文件生成配置，格式示例：
源：
127.0.0.1 127.0.0.2 127.0.0.3
128.0.0.1 128.0.0.2

输出：
127.0.0.1
127.0.0.2 repl_master_ip=127.0.0.1
127.0.0.3 repl_master_ip=127.0.0.2
128.0.0.1
128.0.0.2 repl_master_ip=128.0.0.1
"""

from pathlib import Path
import argparse
import sys

def process_lines(lines):
    out_lines = []
    for line in lines:
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        parts = s.split()
        if not parts:
            continue
        for i, ip in enumerate(parts):
            if i == 0:
                out_lines.append(ip)
            else:
                out_lines.append(f"{ip} repl_master_ip={parts[i-1]}")
    return out_lines

def main():
    parser = argparse.ArgumentParser(description='从源文件生成 repl_master_ip 配置')
    parser.add_argument('-i', '--input', default='source.txt', help='源文件路径，默认 source.txt')
    parser.add_argument('-o', '--output', help='输出文件路径；若不指定则写到标准输出')
    args = parser.parse_args()

    src = Path(args.input)
    if not src.exists():
        print(f'输入文件不存在: {src}', file=sys.stderr)
        sys.exit(2)

    lines = src.read_text(encoding='utf-8').splitlines()
    out = process_lines(lines)

    if args.output:
        Path(args.output).write_text("\n".join(out) + ("\n" if out else ""), encoding='utf-8')
    else:
        sys.stdout.write("\n".join(out) + ("\n" if out else ""))

if __name__ == '__main__':
    main()
