"""远程执行 shell 命令的轻量工具。

从 ssh.py 的 run_ssh 链路提取，去掉了 paramiko / pymysql 等依赖，仅依赖标准库，
兼容 Python 3.7+。底层仍是调用本地 ssh 客户端，凭据走本地当前用户的默认方式
（或通过 password / key_data 指定）。
"""

import fcntl
import getpass
import os
import selectors
import shlex
import subprocess
import tempfile
import time
from typing import Optional, Tuple, Union

CURRENT_USER = getpass.getuser()


def _to_bytes(s: Union[str, bytes], encoding: str = 'utf-8') -> bytes:
    """将 str 转为 bytes，bytes 原样返回。"""
    if isinstance(s, str):
        return s.encode(encoding)
    if isinstance(s, bytes):
        return s
    raise TypeError('not str or bytes: %r' % type(s))


def _to_str(s: Union[str, bytes], encoding: str = 'utf-8') -> str:
    """将 bytes 转为 str，str 原样返回。"""
    if isinstance(s, bytes):
        return s.decode(encoding)
    return s


def _build_command(binary, *other_args, port=22, user=CURRENT_USER, password=None,
                   key_file=None, control_master=True):
    """组装本地 ssh/scp 命令行（bytes 列表）。"""
    assert binary in ('ssh', 'scp')
    b_command = []

    if password:
        b_command += [b'sshpass', b'-P', b'ass', b'-e']
    b_command += (
        _to_bytes(binary),
        b"-o", b'User="%s"' % _to_bytes(user),
        b"-o", b"Port=%s" % _to_bytes(str(port)),
        b"-o", b"ConnectTimeout=3",
        b"-o", b"StrictHostKeyChecking=no",
        b"-o", b"ServerAliveInterval=5",
        b"-o", b"ServerAliveCountMax=3",
    )
    if key_file:
        b_command += (
            b"-o", b"PreferredAuthentications=publickey",
            b'-i', _to_bytes(key_file),
        )
    elif password:
        b_command += (b"-o", b"PreferredAuthentications=password")

    if control_master:
        b_command += (
            b'-o', b'ControlMaster=auto',
            b'-o', b'ControlPersist=1h',
            b"-o", b'ControlPath="/dev/shm/master-%r@%h:%p"',
        )
    else:
        b_command += (b"-o", b'ControlPath=none')

    if other_args:
        b_command += [_to_bytes(a) for a in other_args]

    return b_command


def _bare_run(cmd, env=None, logger=None, timeout=None) -> Tuple[int, str]:
    """启动命令并读取其 stdout/stderr 直到结束。"""
    if logger:
        logger.debug(cmd)
    if env:
        env['LC_ALL'] = 'en_US.UTF-8'
    else:
        env = {'LC_ALL': 'en_US.UTF-8'}
    start_time = time.time()
    p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, env=env)
    b_output = b_stdout = b_stderr = b''

    select_timeout = 4
    for fd in (p.stdout, p.stderr):
        fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK)

    selector = selectors.DefaultSelector()
    selector.register(p.stdout, selectors.EVENT_READ)
    selector.register(p.stderr, selectors.EVENT_READ)
    try:
        while True:
            if timeout is not None and time.time() - start_time > timeout:
                p.kill()
                p.wait()
                raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)
            poll = p.poll()
            events = selector.select(select_timeout)
            for key, event in events:
                if key.fileobj == p.stdout:
                    b_chunk = p.stdout.read()
                    if b_chunk == b'':
                        selector.unregister(p.stdout)
                        select_timeout = 1
                    b_output += b_chunk
                    b_stdout += b_chunk
                    if logger and b_chunk:
                        try:
                            for line in _to_str(b_chunk).rstrip().split('\n'):
                                logger.info(line)
                        except UnicodeDecodeError:
                            logger.info(b_chunk)
                elif key.fileobj == p.stderr:
                    b_chunk = p.stderr.read()
                    if b_chunk == b'':
                        selector.unregister(p.stderr)
                    elif b'muxclient: master hello exchange failed' in b_chunk or \
                            b'mux_client_request_session: read from master failed' in b_chunk or \
                            b'ControlSocket /dev/shm/master-' in b_chunk:
                        if logger:
                            logger.warning(_to_str(b_chunk).rstrip())
                        continue
                    b_output += b_chunk
                    b_stderr += b_chunk
                    if logger and b_chunk:
                        try:
                            for line in _to_str(b_chunk).rstrip().split('\n'):
                                logger.error(line)
                        except UnicodeDecodeError:
                            logger.error(b_chunk)

            if poll is not None:
                if not selector.get_map() or not events:
                    break
                select_timeout = 0
                continue
            elif not selector.get_map():
                p.wait()
                break
    finally:
        selector.close()
        p.stdout.close()
        p.stderr.close()

    if cmd[0] == b'sshpass':
        if p.returncode in (5, 255) and not b_stdout:
            raise ConnectionError(_to_str(b_stderr))
    else:
        if p.returncode == 255 and not b_stdout:
            raise ConnectionError(_to_str(b_stderr))
    return p.returncode, _to_str(b_output)


def _run(binary, args, port=22, user=CURRENT_USER, password=None, key_data=None,
         control_master=True, logger=None, timeout=None) -> Tuple[int, str]:
    """根据凭据类型选择不同的构造方式并执行。"""
    if key_data:
        with tempfile.NamedTemporaryFile(dir='/dev/shm/') as f:
            f.write(_to_bytes(key_data))
            f.flush()
            cmd = _build_command(binary, *args, port=port, user=user,
                                 key_file=f.name, control_master=control_master)
            return _bare_run(cmd, logger=logger, timeout=timeout)
    elif password:
        cmd = _build_command(binary, *args, port=port, user=user, password=password,
                             control_master=control_master)
        return _bare_run(cmd, env={'SSHPASS': password}, logger=logger, timeout=timeout)
    else:
        cmd = _build_command(binary, *args, port=port, user=user,
                             control_master=control_master)
        return _bare_run(cmd, logger=logger, timeout=timeout)


def run_remote(cmd: str, host: str, port: int = 22, user: str = CURRENT_USER,
               password: Optional[str] = None, key_data: Optional[str] = None,
               sudo: bool = False, control_master: bool = True,
               env: Optional[dict] = None, logger=None,
               timeout: Optional[int] = None) -> Tuple[int, str]:
    """在远程主机上执行 shell 命令。

    Args:
        cmd: 要执行的 shell 命令。
        host: 远程主机 IP 或主机名。
        port: ssh 端口，默认 22。
        user: ssh 登录用户，默认本地当前用户。
        password: 密码认证时传入（依赖本地 sshpass）。
        key_data: 私钥内容（字符串），用于公钥认证。
        sudo: 是否以 sudo 方式执行。
        control_master: 是否复用 ssh 连接（ControlMaster）。
        env: 要在远程 session 设置的环境变量。
        logger: 可选的 logging.Logger，用于记录命令与输出。
        timeout: 超时秒数；由于 select 机制，实际可能有约 4s 偏差。

    Returns:
        (return_code, output)，output 为 stdout 与 stderr 合并的文本。

    Raises:
        subprocess.TimeoutExpired: 执行超时。
        ConnectionError: ssh 连接失败（255 且无 stdout）。
    """
    if env:
        cmd = 'export %s\n%s' % (' '.join('%s=%s' % (k, shlex.quote(v)) for k, v in env.items()), cmd)
    if sudo and user != 'root':
        cmd = 'sudo -s <<"ssh_EOF"\n%s\nssh_EOF' % cmd
    args = (host, cmd)
    if logger:
        logger.info('execute command on %s:\n%s' % (host, cmd))
    return _run('ssh', args, port, user, password, key_data, control_master, logger, timeout=timeout)


def run_local(cmd: str, env: Optional[dict] = None, logger=None,
              timeout: Optional[int] = None) -> Tuple[int, str]:
    """在本地执行 shell 命令（通过 /bin/bash -c）。

    支持多行脚本与 heredoc。stdout 与 stderr 合并返回，logger 存在时按行实时输出。

    Args:
        cmd: 要执行的 shell 命令/脚本。
        env: 额外环境变量；不传时仅设置 LC_ALL。
        logger: 可选的 logging.Logger，用于按行记录输出。
        timeout: 超时秒数，超时会 kill 子进程。

    Returns:
        (return_code, output)，output 为 stdout 与 stderr 合并的文本。

    Raises:
        subprocess.TimeoutExpired: 执行超时。
    """
    if logger:
        logger.info('execute local command:\n%s' % cmd)
    full_env = dict(env or {})
    full_env.setdefault('LC_ALL', 'en_US.UTF-8')
    p = subprocess.Popen(cmd, shell=True, executable='/bin/bash',
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=full_env)
    b_output = b''
    start_time = time.time()
    try:
        for raw in iter(p.stdout.readline, b''):
            b_output += raw
            if logger and raw:
                logger.info(raw.rstrip().decode('utf-8', 'replace'))
            if timeout is not None and time.time() - start_time > timeout:
                p.kill()
                p.wait()
                raise subprocess.TimeoutExpired(cmd=cmd, timeout=timeout)
    finally:
        p.stdout.close()
        p.wait()
    return p.returncode, b_output.decode('utf-8', 'replace')


if __name__ == '__main__':
    host = '192.168.0.10'

    # 用法一：单条命令
    code, output = run_remote('hostname', host, sudo=True)
    print('return code:', code)
    print('hostname:', output.strip())

    # 用法二：把整段脚本内容（含 heredoc）直接写在 Python 调用里
    # 注意：sudo 路径下 run_remote 已用 <<ssh_EOF 包裹，脚本里的 heredoc 定界符
    # 不能再用 ssh_EOF，这里用 SCRIPT_EOF 避免冲突。
    script = """\
set -e
cat > /tmp/hello.sh <<'SCRIPT_EOF'
#!/bin/bash
echo "hello from $(hostname)"
echo "now: $(date '+%F %T')"
SCRIPT_EOF
chmod +x /tmp/hello.sh
/tmp/hello.sh
"""
    code, output = run_remote(script, host, sudo=True)
    print('return code:', code)
    print('script output:', output.strip())
