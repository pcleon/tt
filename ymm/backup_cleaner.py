#!/usr/bin/env python3
import os
import re
import logging
import time
import logging.handlers
import subprocess
from datetime import datetime
from dateutil.relativedelta import relativedelta

# 配置部分
BACKUP_DIRS = ["/tmp/backup_test", "/tmp/backup_test2"]  # 可添加多个目录，如 ["/tmp/backup_test", "/tmp/backup_test2"]
LOG_FILE = "/tmp/cleaner.log"

def is_file_in_use(filepath):
    """检查文件是否被占用"""
    try:
        # lsof 返回0表示文件被占用，非0表示未占用
        result = subprocess.run(["lsof", filepath], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return result.returncode == 0
    except Exception:
        return False

def safe_remove(filepath):
    """安全删除文件"""
    if not os.path.exists(filepath):
        return False
        
    try:
        if is_file_in_use(filepath):
            logging.warning(f"文件占用中，跳过删除: {filepath}")
            return False
            
        os.remove(filepath)
        logging.info(f"已删除: {filepath}")
        return True
    except Exception as e:
        logging.error(f"删除失败 {filepath}: {str(e)}")
        return False

def get_date_ranges():
    """获取需要保留文件的日期范围"""
    today = datetime.now()
    # 当月第一天
    current_month_start = today.replace(day=1)
    # 上个月第一天
    last_month_start = (current_month_start - relativedelta(months=1))
    return (last_month_start, current_month_start)

def should_keep_file(file_date):
    """判断文件是否应该保留"""
    last_month_start, current_month_start = get_date_ranges()
    
    # 近两个月内的文件保留
    if file_date >= last_month_start:
        return True
        
    # 超过两个月的文件：仅保留每月1/11/21日
    return file_date.day in (1, 11, 21)

def main():
    # 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.handlers.RotatingFileHandler(
                LOG_FILE, 
                maxBytes=100*1024*1024, 
                backupCount=3,
                encoding='utf-8'
            )
        ]
    )
    
    # 初始化日志
    logging.info("="*50)
    logging.info("开始执行备份清理任务")
    
    total_processed = 0
    total_kept = 0
    total_deleted = 0
    for BACKUP_DIR in BACKUP_DIRS:
        logging.info(f"处理目录: {BACKUP_DIR}")
        try:
            # 支持以 .gz 结尾的多种格式（如 .tar.gz 或 .tar.<hash>.gz）
            files = [f for f in os.listdir(BACKUP_DIR)
                     if f.endswith(".gz") and "_full_" in f]
        except Exception as e:
            logging.error(f"读取备份目录失败: {str(e)}")
            continue

        processed = 0
        kept = 0
        deleted = 0

        for filename in files:
            processed += 1
            file_path = os.path.join(BACKUP_DIR, filename)

            # 按优先级尝试多种模式，确保日期为第1个捕获组
            patterns = [
                r"_full_(\d{12})\.[^.]+\.tar\.gz$",  # ..._YYYYMMDDHHMM.<hash>.tar.gz
                r"_full_(\d{12})\.tar\.[^.]+\.gz$",  # ..._YYYYMMDDHHMM.tar.<hash>.gz
                r"_full_(\d{12})\.tar\.gz$",         # ..._YYYYMMDDHHMM.tar.gz
                r"_full_(\d{12})\.gz$",               # ..._YYYYMMDDHHMM.gz
            ]
            m = None
            for p in patterns:
                m = re.search(p, filename)
                if m:
                    break

            if not m:
                logging.warning(f"无效文件名格式: {filename}")
                continue

            try:
                date_str = m.group(1)
                file_date = datetime.strptime(date_str, "%Y%m%d%H%M")
                file_date = file_date.replace(hour=0, minute=0, second=0)
            except ValueError:
                logging.warning(f"无效日期格式: {filename}")
                continue

            if should_keep_file(file_date):
                kept += 1
                continue

            if safe_remove(file_path):
                deleted += 1
                time.sleep(2)

        logging.info(f"目录处理完成: 总数={processed}, 保留={kept}, 删除={deleted}")
        total_processed += processed
        total_kept += kept
        total_deleted += deleted

    logging.info(f"全部处理完成: 总数={total_processed}, 保留={total_kept}, 删除={total_deleted}")
    logging.info("="*50)

if __name__ == "__main__":
    main()
