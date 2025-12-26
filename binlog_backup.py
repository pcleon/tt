import os
import gzip
import shutil
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

def backup_with_compression(filename):
    """执行备份：格式化时间戳后缀 + 极致压缩"""
    src_path = os.path.join(BINLOG_DIR, filename)
    if not os.path.exists(src_path):
        return

    # 生成时间字符串
    time_suffix = get_human_mtime(src_path)
    dst_filename = f"{filename}.{time_suffix}.gz"
    dst_path = os.path.join(BACKUP_DEST, dst_filename)

    # 检查是否已备份过
    if os.path.exists(dst_path):
        return

    try:
        logging.info(f"正在进行最大化压缩备份: {filename} -> {dst_filename}")
        
        # 流式读取并压缩写入 NFS
        with open(src_path, 'rb') as f_in:
            with gzip.open(dst_path, 'wb', compresslevel=COMPRESS_LEVEL) as f_out:
                shutil.copyfileobj(f_in, f_out)
        
        logging.info(f"备份成功: {dst_filename}")
    except Exception as e:
        logging.error(f"备份出错 {filename}: {e}")
        if os.path.exists(dst_path):
            os.remove(dst_path)

def main():
    if not os.path.exists(BACKUP_DEST):
        logging.error(f"错误: NFS 挂载目录 {BACKUP_DEST} 不存在")
        return

    logging.info("MySQL Binlog 准实时备份脚本启动 (易读时间戳 + Gzip 9)")
    
    while True:
        try:
            for log in get_finished_logs():
                backup_with_compression(log)
        except Exception as e:
            logging.error(f"主程序异常: {e}")
        
        time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    main()