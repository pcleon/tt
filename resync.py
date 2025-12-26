from datetime import datetime, timedelta
import os
import glob
import subprocess
import logging
import socket
import calendar

# 使用当前时间
local_path = "/data/3306/mybackup/my3306/clone"
gfs_path = "/data/3306/mybackup/gfs/clone"


def fillup_miss_file():
    now = datetime.now()
    # 只在每月1号执行
    if now.day != 1:
        return
    hostname = socket.gethostname()
    # 设置日志
    logging.basicConfig(
        filename=os.path.join(local_path, "fillup.log"),
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # 计算上个月
    last_month_date = now.replace(day=1) - timedelta(days=1)
    last_month = last_month_date.month
    year = last_month_date.year

    # 上个月的天数
    days_in_last_month = calendar.monthrange(year, last_month)[1]

    # 上个月的日期范围
    start_date = datetime(year, last_month, 1).date()
    end_date = datetime(year, last_month, days_in_last_month).date()

    # 扫描 gfs_path 目录下的所有 .tar.gz 文件
    files = glob.glob(
        os.path.join(gfs_path, f"*{hostname}_full_{year}{last_month}*.tar.gz")
    )

    # 提取信息
    relevant_files = []
    for file_path in files:
        filename = os.path.basename(file_path)
        parts = filename.split("_")
        ip, host, typ, timestamp_md5_ext = (
            parts[0],
            parts[1],
            parts[2],
            "_".join(parts[3:]),
        )
        # 解析时间戳
        timestamp_parts = timestamp_md5_ext.split(".")
        timestamp = timestamp_parts[0]
        file_date = datetime.strptime(timestamp[:8], "%Y%m%d").date()
        relevant_files.append((file_path, file_date, timestamp, timestamp_parts))

    # 获取存在的日期
    existing_days = set(f[1].day for f in relevant_files)

    # 目标日期
    target_days = [1, 11, 21]

    for day in target_days:
        if day not in existing_days:
            # 找向后的最近一天
            next_day = day + 1
            while next_day <= days_in_last_month:
                if next_day in existing_days:
                    # 找到文件
                    for (
                        file_path,
                        file_date,
                        timestamp,
                        timestamp_parts,
                    ) in relevant_files:
                        if file_date.day == next_day:
                            # 重命名
                            new_date = file_date.replace(day=day)
                            new_timestamp = (
                                new_date.strftime("%Y%m%d") + timestamp[8:]
                            )  # 保持时分秒
                            new_timestamp_md5_ext = (
                                new_timestamp + "." + ".".join(timestamp_parts[1:])
                            )
                            # 构建新文件名
                            parts = os.path.basename(file_path).split("_")
                            parts[3] = new_timestamp_md5_ext
                            new_filename = "_".join(parts)
                            new_path = os.path.join(
                                os.path.dirname(file_path), new_filename
                            )
                            os.rename(file_path, new_path)
                            logging.info(
                                f"Renamed {os.path.basename(file_path)} to {new_filename}"
                            )
                            print(
                                f"Renamed {os.path.basename(file_path)} to {new_filename}"
                            )
                            break
                    break
                next_day += 1
            else:
                print(f"No file found after day {day} for {hostname}")


def resync_file(local_path, gfs_path):
    today = datetime.now().date()

    dates = []
    for i in range(1, 5):  # 0: 今天, 1: 昨天, 2: 前天, 3: 大前天
        date = today - timedelta(days=i)
        dates.append(date.strftime("%Y%m%d"))

    print("日期列表:", dates)

    # 查找local_path目录中以.gz结尾的文件
    gz_files = glob.glob(os.path.join(local_path, "*.gz"))

    matched_files = []
    for file in gz_files:
        filename = os.path.basename(file)
        # 检查文件名是否包含任何一个日期
        if any(f"_full_{date}" in filename for date in dates):
            matched_files.append(filename)

    print("匹配的文件:", matched_files)

    # 设置日志
    logging.basicConfig(
        filename="/tmp/copy.log",
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    # 对于匹配的文件，如果gfs_path中没有，则rsync拷贝
    for filename in matched_files:
        local_file = os.path.join(local_path, filename)
        gfs_file = os.path.join(gfs_path, filename)
        if not os.path.exists(gfs_file):
            result = subprocess.run(
                ["rsync", "-av", local_file, gfs_file],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
            if result.returncode == 0:
                logging.info(f"Successfully copied {filename} to {gfs_path}")
                print(f"已拷贝: {filename}")
            else:
                logging.error(f"Failed to copy {filename}: {result.stderr}")
                print(f"拷贝失败: {filename} - {result.stderr}")


# resync_file(local_path, gfs_path)
fillup_miss_file()
