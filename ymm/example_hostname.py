"""对远程主机 192.168.0.10 执行 sudo hostname 的示例。"""

from ssh import ServerRemoteExecute


def main() -> None:
    """在远程主机上执行 sudo hostname 并打印结果。"""
    host = '192.168.0.10'
    # run_ssh 为静态方法，直接用本地当前用户的 ssh 凭据连接，无需 DB 元数据
    ret_code, output = ServerRemoteExecute.run_ssh('hostname', host, sudo=True)
    print(f'return code: {ret_code}')
    print(f'hostname: {output.strip()}')


if __name__ == '__main__':
    main()
