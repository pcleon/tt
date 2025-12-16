import fcntl
from functools import partial
import getpass
import io
import logging
import os
from pathlib import Path, PurePath
import random
import selectors
import shlex
import string
import subprocess
import tempfile
import textwrap
import time
from typing import Union
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pymysql
import paramiko
from paramiko.channel import Channel
from paramiko.common import MSG_CHANNEL_SUCCESS, MSG_CHANNEL_FAILURE, MSG_CHANNEL_DATA, MSG_CHANNEL_EXTENDED_DATA, \
    MSG_CHANNEL_WINDOW_ADJUST, MSG_CHANNEL_REQUEST, MSG_CHANNEL_EOF, MSG_CHANNEL_CLOSE
from paramiko.util import u as to_str
from paramiko.util import b as to_bytes
from dotenv import load_dotenv


# 加载环境变量
load_dotenv()

CURRENT_USER = getpass.getuser()

DB = {
    'user': os.getenv('MYSQL_USER', ""),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'host': os.getenv('MYSQL_HOST', "127.0.0.1"),
    'port': int(os.getenv('MYSQL_PORT', "3306")),
    'database': os.getenv('DB_NAME', 'test').lower()
}

class myHook(pymysql.Connection):

    def __init__(self):
        super().__init__(**DB, charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        self._log = None

    @property
    def log(self) -> logging.Logger:
        """Returns a logger."""
        if self._log is None:
            self._log = logging.getLogger('myHook')
        return self._log

    def get_conn(self):
        return self

    def fetchone(self, sql: str, params=None):
        """Execute SQL query and return the first row."""
        with self.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchone()

    def fetchall(self, sql: str, params=None):
        """Execute SQL query and return all rows."""
        with self.cursor() as cursor:
            cursor.execute(sql, params)
            return cursor.fetchall()

myhook = myHook()


def get_remote_working_dir(hook=myhook):
    # return hook.get_first("select value from oat_configproperty where `key`='dba.remote_working_dir'")['value']
    return '/tmp'

def get_random_string(length=8, choices=string.ascii_letters + string.digits):
    """Generate random string."""
    return "".join(random.choices(choices, k=length))

def _feed(self, m, logger=None):
    if isinstance(m, bytes):
        # passed from _feed_extended
        s = m
    else:
        s = m.get_binary()
        for line in to_str(s).rstrip().split('\n'):
            logger.info(line.rstrip())  # trim /n
    self.in_buffer.feed(s)


def _feed_extended(self, m, logger=None):
    code = m.get_int()
    s = m.get_binary()
    if code != 1:
        self._log(
            paramiko.common.ERROR, "unknown extended_data type {}; discarding".format(code)
        )
        return
    for line in to_str(s).rstrip().split('\n'):
        logger.error(line.rstrip())
    if self.combine_stderr:
        self._feed(s)
    else:
        self.in_stderr_buffer.feed(s)
class ServerRemoteExecute(object):
    def __init__(self, server_instance, hook=myhook):
        self.hook = hook
        self.server_instance = server_instance
        self.server = self._get_server()
        self._client = None

    def close(self):
        if self._client:
            self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_active_client(self):
        server = self.server
        if self._client is None:
            self._client = self.get_ssh_client_by_credential(
                server['ip'], server['ssh_port'], server['username'], server['auth_type'],
                server['password'], server['key_data'], server['passphrase'], encrypted=False
            )
        elif not self._client.get_transport().active:
            self._client.close()
            self._client = self.get_ssh_client_by_credential(
                server['ip'], server['ssh_port'], server['username'], server['auth_type'],
                server['password'], server['key_data'], server['passphrase'], encrypted=False
            )
        return self._client

    def _get_server(self):
        sql = "SELECT a.id, a.idc, a.cluster_name, a.cluster_vip_port, b.instance_name, b.ip, " \
              "b.instance_role, instance_read_only FROM mysql_cluster_instance b " \
              "JOIN mysql_cluster a ON a.cluster_name = b.cluster_name " \
              "WHERE b.instance_name = %s "
        server = self.hook.fetchone(sql=sql, params=[self.server_instance])
        assert server is not None, f'server {self.server_instance} not found or credential not set'
        server = {
            "id": server['id'],
            "idc": server['idc'],
            "ip": server['ip'],
            "ssh_port": 22,
            "username": os.getenv('ssh_user', CURRENT_USER),
            "auth_type": "pubkey"
        }
        return {
            **server
        }

    def get_server_idc(self):
        sql = "SELECT a.id, a.idc idc, a.cluster_name,  b.instance_name, b.ip, " \
              "b.instance_role, instance_read_only FROM mysql_cluster_instance b " \
              "JOIN mysql_cluster a ON a.cluster_name = b.cluster_name " \
              "WHERE b.instance_name = %s "
        idc = self.hook.get_first(sql=sql, parameters=[self.server_instance])
        assert idc is not None, f'server {self.server_instance} not found or idc is empty'
        return idc

    @classmethod
    def get_ssh_client_by_credential(cls, host, ssh_port, username, auth_type, password='', key_data='',
                                     passphrase=''):
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.MissingHostKeyPolicy())  # 不匹配knowhost，done nothing
        try:
            sock = None
            if auth_type == 'external':
                client.connect(host, port=ssh_port, username=username, timeout=2, look_for_keys=True, sock=sock)
            elif auth_type == 'pubkey':
                client.connect(host, port=ssh_port, username=username,
                               pkey=paramiko.RSAKey.from_private_key(io.StringIO(key_data)),
                               passphrase=passphrase,
                               timeout=2, look_for_keys=False, sock=sock)
            elif auth_type == 'password':
                client.connect(host, port=ssh_port, username=username, password=password,
                               timeout=2, look_for_keys=False, sock=sock)
            else:
                raise RuntimeError('not support credential auth type: %s' % auth_type)
        except Exception as e:
            raise AssertionError(str(e))
        return client

    @classmethod
    def call_remote_execute(cls, client, cmd, sudo=False, timeout=3600, env=None, logger=None):
        """deprecated"""
        transport = client.get_transport()
        if sudo and transport.get_username() != 'root':
            cmd = 'sudo -s <<"OAT_REMOTE_EXECUTE_EOF"\n%s\nOAT_REMOTE_EXECUTE_EOF' % cmd

        if logger:
            transport._channel_handler_table = {
                MSG_CHANNEL_SUCCESS: Channel._request_success,
                MSG_CHANNEL_FAILURE: Channel._request_failed,
                MSG_CHANNEL_DATA: partial(_feed, logger=logger),
                MSG_CHANNEL_EXTENDED_DATA: partial(_feed_extended, logger=logger),
                MSG_CHANNEL_WINDOW_ADJUST: Channel._window_adjust,
                MSG_CHANNEL_REQUEST: Channel._handle_request,
                MSG_CHANNEL_EOF: Channel._handle_eof,
                MSG_CHANNEL_CLOSE: Channel._handle_close,
            }
            host = transport.sock.getpeername()[0]
            logger.info(f'execute command on {host}:\n{cmd}')
        buf_size = 4096
        chan = transport.open_session(timeout=timeout)
        chan.settimeout(timeout)
        chan.set_combine_stderr(True)
        if env:
            chan.update_environment(env)
        chan.exec_command(cmd)
        ret_code = chan.recv_exit_status()
        stdout = to_str(chan.makefile("r", buf_size).read())
        if logger:
            del transport._channel_handler_table
        return ret_code, stdout

    def remote_write(self, content, remote_file_name: str, logger=None):
        cat_cmd = textwrap.dedent('''\
        cat > %(file_name)s << __OAT_REMOTE_WRITE__
        %(file_content)s
        __OAT_REMOTE_WRITE__
        ''') % {
            'file_name': remote_file_name,
            'file_content': content
        }
        status_code, output = self.execute(cat_cmd, logger=logger)
        if status_code != 0:
            raise RuntimeError(f'write file failed: {output}')

    def remote_execute(self, cmd, sudo=False, timeout=None, env=None, logger=None):
        """deprecated: 不再使用paramiko, 直接使用 execute 函数"""
        return self.execute(cmd, sudo=sudo, env=env, logger=logger, timeout=timeout)


    @staticmethod
    def _build_command(binary, *other_args, port=22, user=CURRENT_USER, password=None, key_file=None, control_master=True):
        """
        Takes a executable (ssh, scp, sftp or wrapper) and optional extra arguments and returns the remote command
        wrapped in local ssh shell commands and ready for execution.
        :arg other_args: dict of, value pairs passed as arguments to the ssh binary
        """
        assert binary in ('ssh', 'scp')
        b_command = []

        if password:
            b_command += [b'sshpass', b'-P', b'ass', b'-e']
        # ssh parameters
        b_command += (
            to_bytes(binary),
            b"-o", b'User="%s"' % to_bytes(user),
            b"-o", b"Port=%s" % to_bytes(str(port)),
            b"-o", b"ConnectTimeout=3",
            b"-o", b"StrictHostKeyChecking=no",
            b"-o", b"ServerAliveInterval=5",
            b"-o", b"ServerAliveCountMax=3",
        )
        if key_file:
            b_command += (
                b"-o", b"PreferredAuthentications=publickey",
                b'-i', to_bytes(key_file),
            )
        elif password:
            b_command += (
                b"-o", b"PreferredAuthentications=password",
            )
        else:
            pass  # external using default

        if control_master:
            b_command += (
                b'-o', b'ControlMaster=auto',
                b'-o', b'ControlPersist=1h',
                b"-o", b'ControlPath="/dev/shm/master-%r@%h:%p"'
            )
        else:
            b_command += (b"-o", b'ControlPath=none')

        if other_args:
            b_command += [to_bytes(a) for a in other_args]

        return b_command

    @staticmethod
    def _bare_run(cmd, env=None, logger=None, timeout=None):
        """Starts the command and communicates with it until it ends."""
        if logger:
            logger.debug(cmd)
        if env:
            env['LC_ALL'] = 'en_US.UTF-8'
        else:
            env = {'LC_ALL': 'en_US.UTF-8'}
        start_time = time.time()
        p = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
        b_output = b_stdout = b_stderr = b''

        # select timeout should be longer than the connect timeout, otherwise
        # they will race each other when we can't connect, and the connect
        # timeout usually fails
        select_timeout = 4
        for fd in (p.stdout, p.stderr):
            fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK)

        # select is faster when filehandles is low and we only ever handle 1.
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
                # Read whatever output is available on stdout and stderr, and stop
                # listening to the pipe if it's been closed.
                for key, event in events:
                    if key.fileobj == p.stdout:
                        b_chunk = p.stdout.read()
                        if b_chunk == b'':
                            # stdout has been closed, stop watching it
                            selector.unregister(p.stdout)
                            # When ssh has ControlMaster (+ControlPath/Persist) enabled, the
                            # first connection goes into the background and we never see EOF
                            # on stderr. If we see EOF on stdout, lower the select timeout
                            # to reduce the time wasted selecting on stderr if we observe
                            # that the process has not yet existed after this EOF. Otherwise
                            # we may spend a long timeout period waiting for an EOF that is
                            # not going to arrive until the persisted connection closes.
                            select_timeout = 1
                        b_output += b_chunk
                        b_stdout += b_chunk
                        if logger and b_chunk:
                            try:
                                for line in to_str(b_chunk).rstrip().split('\n'):
                                    logger.info(line)
                            except UnicodeDecodeError:
                                logger.info(b_chunk)
                    elif key.fileobj == p.stderr:
                        b_chunk = p.stderr.read()
                        if b_chunk == b'':
                            # stderr has been closed, stop watching it
                            selector.unregister(p.stderr)
                        elif b'muxclient: master hello exchange failed' in b_chunk or \
                            b'mux_client_request_session: read from master failed' in b_chunk or \
                            b'ControlSocket /dev/shm/master-' in b_chunk:
                            # ControlSocket /dev/shm/master-root@10.100.15.140:22 already exists, disabling multiplexing
                            if logger:
                                logger.warning(to_str(b_chunk).rstrip())
                            continue
                        b_output += b_chunk
                        b_stderr += b_chunk
                        if logger and b_chunk:
                            try:
                                for line in to_str(b_chunk).rstrip().split('\n'):
                                    logger.error(line)
                            except UnicodeDecodeError:
                                logger.error(b_chunk)

                if poll is not None:
                    if not selector.get_map() or not events:
                        break
                    # We should not see further writes to the stdout/stderr file
                    # descriptors after the process has closed, set the select
                    # timeout to gather any last writes we may have missed.
                    select_timeout = 0
                    continue

                # If the process has not yet exited, but we've already read EOF from
                # its stdout and stderr (and thus no longer watching any file
                # descriptors), we can just wait for it to exit.

                elif not selector.get_map():
                    p.wait()
                    break

                # Otherwise there may still be outstanding data to read.
        finally:
            selector.close()
            p.stdout.close()
            p.stderr.close()

        if cmd[0] == b'sshpass':
            if p.returncode in (5, 255) and not b_stdout:
                raise ConnectionError(to_str(b_stderr))
        else:
            if p.returncode == 255 and not b_stdout:
                raise ConnectionError(to_str(b_stderr))
        return p.returncode, to_str(b_output)

    @staticmethod
    def _run(binary, args, port=22, user=CURRENT_USER, password=None, key_data=None, control_master=True, logger=None, timeout=None):
        """处理不同的密码逻辑"""
        if key_data:
            with tempfile.NamedTemporaryFile(dir='/dev/shm/') as f:
                f.write(to_bytes(key_data))
                f.flush()
                cmd = ServerRemoteExecute._build_command(binary, *args, port=port, user=user, key_file=f.name, control_master=control_master)
                return ServerRemoteExecute._bare_run(cmd, logger=logger, timeout=timeout)
        elif password:
            cmd = ServerRemoteExecute._build_command(binary, *args, port=port, user=user, password=password, control_master=control_master)
            return ServerRemoteExecute._bare_run(cmd, env={'SSHPASS': password}, logger=logger, timeout=timeout)
        else:
            cmd = ServerRemoteExecute._build_command(binary, *args, port=port, user=user, control_master=control_master)
            return ServerRemoteExecute._bare_run(cmd, logger=logger, timeout=timeout)

    @staticmethod
    def run_ssh(cmd, host, port=22, user=CURRENT_USER, password=None, key_data=None, sudo=False, control_master=True, env=None, logger=None, timeout=None):
        """ run a command on the remote host, env is set on remote session """
        if env:
            cmd = 'export %s\n%s' % (' '.join(f'{k}={shlex.quote(v)}' for k, v in env.items()), cmd)
        if sudo and user != 'root':
            cmd = 'sudo -s <<"ssh_EOF"\n%s\nssh_EOF' % cmd
        args = (host, cmd)
        if logger:
            logger.info(f'execute command on {host}:\n{cmd}')
        return ServerRemoteExecute._run('ssh', args, port, user, password, key_data, control_master, logger, timeout=timeout)

    @staticmethod
    def run_scp(local_path: Path, host, port=22, user=CURRENT_USER, password=None, key_data=None, control_master=True,
                logger=None, remote_path: Union[str, PurePath] = None):
        """ copy file(s) to remote host """
        if not remote_path:
            remote_temp_name = PurePath(get_remote_working_dir()).joinpath(local_path.name + get_random_string())
        else:
            remote_temp_name = remote_path

        if not local_path.exists():
            raise RuntimeError(f'local path {local_path} not exists')
        if local_path.is_dir():
            scp_args = ('-r', str(local_path), '{0}:{1}'.format(host, remote_temp_name))
        else:
            scp_args = (str(local_path), '{0}:{1}'.format(host, remote_temp_name))
        ret_code, output = ServerRemoteExecute._run('scp', scp_args, port, user, password, key_data, control_master, logger)
        if ret_code != 0:
            raise RuntimeError('scp failed: %s' % output)

    def execute(self, cmd, sudo=False, control_master=True, env=None, logger=None, timeout: int=None) -> (int, str):
        """由于 _bare_run 实现机制的原因，timeout 最大可能会有 select_timeout (4s) 的偏差，影响不大 """
        return self.run_ssh(
            cmd, self.server['ip'], self.server['ssh_port'], self.server['username'], 
            sudo=sudo, control_master=control_master, env=env, logger=logger, timeout=timeout
        )

    def scp_(self, local_path: Union[str, Path], remote_path: Union[str, PurePath] = None, logger=None) -> PurePath:
        if isinstance(local_path, str):
            local_path = Path(local_path)
        if isinstance(remote_path, str):
            remote_path = PurePath(remote_path)
        if not remote_path:
            remote_path = PurePath(get_remote_working_dir()).joinpath(local_path.name + get_random_string())
        if self.server['username'] != 'root' and not remote_path.is_relative_to(get_remote_working_dir()):
            tmp_dir = f'{get_remote_working_dir()}/.dba.{get_random_string()}'
            self.execute(f'mkdir -p {tmp_dir} && chmod 777 {tmp_dir}', logger=logger)
            self.run_scp(
                local_path, self.server['ip'], self.server['ssh_port'], self.server['username'],
                logger=logger, remote_path=tmp_dir
            )
            self.execute(f'\\cp -rf {tmp_dir}/* {remote_path} && rm -rf {tmp_dir}', logger=logger)
        else:
            os.chmod(local_path, 0o755)
            self.run_scp(
                local_path, self.server['ip'], self.server['ssh_port'], self.server['username'], 
                logger=logger, remote_path=remote_path
            )
        return remote_path

    def execute_script(self, local_path: Path, args=(), sudo=False, control_master=True, env=None, logger=None,
                       remote_path: Union[str, PurePath] = None):
        remote_temp_name = self.scp_(local_path, remote_path=remote_path, logger=logger)
        cmd = f'{remote_temp_name} {" ".join(shlex.quote(p) for p in args)}; ret=$?; rm -f {remote_temp_name}; exit $ret;'
        return self.run_ssh(
            cmd, self.server['ip'], self.server['ssh_port'], self.server['username'],
            sudo=sudo, control_master=control_master, env=env, logger=logger
        )


if __name__ == '__main__':
    with ServerRemoteExecute('pc_box') as remote:
        code, output = remote.execute('df -h; whoami ',sudo=True, logger=logging.getLogger())
        # code, output = remote.execute_script(Path('/tmp/test.sh'), args=('/', '/home'), logger=logging.getLogger())
        print(code)
        print(output)