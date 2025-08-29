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
BACKUP_DIR = "/tmp/backup_test"
LOG_FILE = "/tmp/backup_cleaner.log"

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
    
    # 获取所有备份文件
    try:
        files = [f for f in os.listdir(BACKUP_DIR) 
                 if f.endswith(".tar.gz") and "_full_" in f]
    except Exception as e:
        logging.error(f"读取备份目录失败: {str(e)}")
        return
    
    # 文件计数器
    processed = 0
    kept = 0
    deleted = 0
    
    for filename in files:
        processed += 1
        file_path = os.path.join(BACKUP_DIR, filename)
        
        # 解析文件日期（匹配12位日期：YYYYMMDDHHMM）
        match = re.search(r"_full_(\d{12})\.\w+\.tar\.gz$", filename)
        if not match:
            logging.warning(f"无效文件名格式: {filename}")
            continue
            
        try:
            # 解析包含时分的日期，并归一化到日期（清除时间部分）
            file_date = datetime.strptime(match.group(1), "%Y%m%d%H%M")
            file_date = file_date.replace(hour=0, minute=0, second=0)
        except ValueError:
            logging.warning(f"无效日期格式: {filename}")
            continue
            
        # 判断是否保留
        if should_keep_file(file_date):
            kept += 1
            continue
            
        # 执行删除
        if safe_remove(file_path):
            deleted += 1
            time.sleep(2)
    
    # 生成报告
    logging.info(f"处理完成: 总数={processed}, 保留={kept}, 删除={deleted}")
    logging.info("="*50)

if __name__ == "__main__":
    main()
