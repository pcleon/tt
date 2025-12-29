import os
import subprocess
import time
import logging
from datetime import datetime

# --- 配置参数 ---
BINLOG_DIR = "/data/3306/mysql/data"
INDEX_FILE = "mysql-bin.index"
BACKUP_DEST = "/data/3306/mybackup/gfs/binlog_backup/{hostname}"
CHECK_INTERVAL = 30
COMPRESS_LEVEL = 9  # 1-9, 9是最高压缩率

# 日志配置
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def get_human_mtime(file_path):
    """获取文件修改时间并转为人类易读格式: YYYYMMDD_HHMMSS"""
    mtime_ts = os.path.getmtime(file_path)
    return datetime.fromtimestamp(mtime_ts).strftime('%Y%m%d_%H%M%S')


def get_finished_logs():
    """解析索引文件，获取已写完的 binlog 列表"""
    index_path = os.path.join(BINLOG_DIR, INDEX_FILE)
    if not os.path.exists(index_path):
        logging.error(f"找不到索引文件: {index_path}")
        return []

    try:
        with open(index_path, 'r') as f:
            lines = [line.strip().replace('./', '') for line in f if line.strip()]
        # 排除当前正在写入的最后一个文件
        return lines[:-1] if len(lines) > 1 else []
    except Exception as e:
        logging.error(f"读取索引文件失败: {e}")
        return []


def backup_using_system_gzip(filename):
    """调用系统命令进行压缩和备份"""
    src_path = os.path.join(BINLOG_DIR, filename)
    if not os.path.exists(src_path):
        return

    # 获取易读的时间戳作为后缀
    mtime_ts = os.path.getmtime(src_path)
    time_suffix = datetime.fromtimestamp(mtime_ts).strftime('%Y%m%d%H%M%S')
    dst_filename = f"{filename}.{time_suffix}.gz"
    dst_path = os.path.join(BACKUP_DEST, dst_filename)

    # 检查 NFS 中是否已存在（避免重复备份）
    if os.path.exists(dst_path):
        return

    try:
        logging.info(f"备份: {filename}")
        # 核心命令: gzip -9 -c [源文件] > [NFS目标文件]
        # -c 表示将压缩结果输出到标准输出，通过重定向 > 写入 NFS，一步到位
        cmd = f"gzip -{COMPRESS_LEVEL} -c {src_path} > {dst_path}"
        # 使用 shell=True 以支持重定向符 '>'
        result = subprocess.run(cmd, shell=True, check=True, stderr=subprocess.PIPE)
        logging.info(f"备份完成: {dst_filename}")
    except subprocess.CalledProcessError as e:
        error_msg = e.stderr.decode().strip()
        logging.error(f"系统命令执行失败: {error_msg}")
        # 清理可能产生的残缺文件
        if os.path.exists(dst_path):
            os.remove(dst_path)


def main():
    if not os.path.exists(BACKUP_DEST):
        logging.error(f"错误: NFS 挂载目录 {BACKUP_DEST} 不存在")
        return

    logging.info("MySQL Binlog 准实时备份脚本启动")
    while True:
        try:
            for log in get_finished_logs():
                backup_using_system_gzip(log)
        except Exception as e:
            logging.error(f"主程序异常: {e}")
        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
