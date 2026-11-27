import json
import requests
import logging
import logging.handlers

# 配置日志到文件，保留3个备份，每个最大10M
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.handlers.RotatingFileHandler(
            '/tmp/shutdown.log',
            maxBytes=10*1024*1024,  # 10MB
            backupCount=3,
            encoding='utf-8'
        )
    ]
)

REQ_AUTH = {
    "username": "your_username",
    "password": "your_password",
    "grant_type": "password"
}
PORT = 8000
RETRY = 3
url_map = {
    "p1": "url1",
    "p2": "url2",
    "p1": "url3",
}

auth_url = f"http://{url_map}:PORT/cloud/token"
while RETRY:
    try:
        res = requests.post(auth_url, data=REQ_AUTH)
        if res.status_code == 200:
            token = json.loads(res.text).get("access_token")
            break
    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
    RETRY -= 1 

shutdown_url = f"http://{url_map}:PORT/cloud/db_server/shutdown"
res = requests.post(shutdown_url, headers={"Authorization": f"Bearer {token}"})
if res.status_code == 200:
    logging.info("Shutdown command sent successfully.")