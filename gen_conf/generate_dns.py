#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从源文件生成 DNS A 记录行。

源示例：
db1 127.0.0.1 127.0.0.2 127.0.0.3
db2 128.0.0.1 128.0.0.2

默认输出：
ylp1-db1-01.local.xx.int. IN A 127.0.0.1
ylp1-db1-02.local.xx.int. IN A 127.0.0.2
ylp1-db1-03.local.xx.int. IN A 127.0.0.3
ylp1-db2-01.local.xx.int. IN A 128.0.0.1
ylp1-db2-02.local.xx.int. IN A 128.0.0.2

可通过参数自定义前缀、域名和序号宽度。
"""

from pathlib import Path
import argparse
import sys


def process_lines(lines, prefix='ylp1-', domain='local.xx.int.', pad=2, sep='-'):
    out = []
    # normalize domain to have no leading/trailing spaces and ensure it ends with a dot
    domain = domain.strip()
    if not domain.endswith('.'):
        domain = domain + '.'

    for line in lines:
        s = line.strip()
        if not s or s.startswith('#'):
            continue
        parts = s.split()
        if not parts:
            continue
        db = parts[0]
        ips = parts[1:]
        if not ips:
            continue
        for i, ip in enumerate(ips, start=1):
            idx = str(i).zfill(pad)
            host = f"{prefix}{db}{sep}{idx}.{domain}"
            out.append(f"{host} IN A {ip}")
    return out


def main():
    p = argparse.ArgumentParser(description='从源文件生成 DNS A 记录')
    p.add_argument('-i', '--input', default='source.txt', help='源文件路径，默认 source.txt')
    p.add_argument('-o', '--output', help='输出文件路径（不指定则写标准输出）')
    p.add_argument('--prefix', default='ylp1-', help='主机名前缀，默认 "ylp1-"')
    p.add_argument('--domain', default='local.xx.int.', help='域名，默认 "local.xx.int."')
    p.add_argument('--pad', type=int, default=2, help='序号宽度，默认 2（01,02..）')
    p.add_argument('--sep', default='-', help='db 名称与序号之间的分隔符，默认 "-"')
    args = p.parse_args()

    src = Path(args.input)
    if not src.exists():
        print(f'输入文件不存在: {src}', file=sys.stderr)
        sys.exit(2)

    lines = src.read_text(encoding='utf-8').splitlines()
    out_lines = process_lines(lines, prefix=args.prefix, domain=args.domain, pad=args.pad, sep=args.sep)

    txt = "\n".join(out_lines) + ("\n" if out_lines else "")
    if args.output:
        Path(args.output).write_text(txt, encoding='utf-8')
    else:
        sys.stdout.write(txt)


if __name__ == '__main__':
    main()
