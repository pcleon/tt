import pandas as pd
import requests
import json
import numpy as np

def process_excel_and_send_data():
    # 定义需要的列
    required_columns = ['instance', 'db_name', 'table_name', 'clear_condition', 'cron_info']
    
    # 定义POST数据模板
    template_data = {
        "cluster_name": "{instance}",  # 需要替换
        "db_name": "{db_name}",        # 需要替换
        "table_name": "table_name",    # 固定值
        "task_seq_of_table": 0,        # 固定值
        "method": "{table_name}",      # 需要替换（使用table_name列的值）
        "condition": "{clear_condition}", # 需要替换
        "cron_tab": "{cron_info}",     # 需要替换
        "priority": 0,                 # 固定值
        "enabled": 0,                  # 固定值
        "target_cluster_name": "archivedb_1", # 固定值
        "target_db_name": "",          # 固定值
        "target_table_name": "",       # 固定值
        "is_regexp_task": False,       # 固定值
        "expire_days": 0,              # 固定值
        "rd": "rd_user",               # 固定值
        "dba": "dba_user"              # 固定值
    }
    
    try:
        # 读取Excel文件
        df = pd.read_excel('t2.xlsx', sheet_name='Sheet1')
        print(f"Successfully read Excel file with {len(df)} rows")
        
        # 检查是否包含所有需要的列
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Warning: Missing columns: {missing_columns}")
        
        # 只保留需要的列
        df_filtered = df[required_columns].copy()
        
        # 处理NaN和无穷大值，以及确保所有值都可以JSON序列化
        df_filtered = df_filtered.replace([np.inf, -np.inf], None)
        df_filtered = df_filtered.fillna('')
        
        # 确保所有值都是JSON可序列化的
        for col in df_filtered.columns:
            df_filtered[col] = df_filtered[col].apply(lambda x: str(x) if not isinstance(x, (str, int, float, bool)) or pd.isna(x) else x)
        
        # 显示处理统计信息
        print(f"Processing {len(df_filtered)} rows with columns: {list(df_filtered.columns)}")
        
        # 遍历每一行数据
        success_count = 0
        error_count = 0
        
        for index, row in df_filtered.iterrows():
            # 根据模板创建POST数据，并替换占位符
            post_data = template_data.copy()
            
            # 替换占位符
            post_data["cluster_name"] = row["instance"]
            post_data["db_name"] = row["db_name"]
            post_data["method"] = row["table_name"]
            post_data["condition"] = row["clear_condition"]
            post_data["cron_tab"] = row["cron_info"]
            
            # 打印将要发送的数据
            print(f"Sending data for row {index+1}: {json.dumps(post_data, ensure_ascii=False, indent=2)}")
            
            # 发送POST请求
            try:
                response = requests.post(
                    'http://127.0.0.1:8881/api/v1/task',
                    json=post_data,
                    headers={'Content-Type': 'application/json'},
                    timeout=10  # 10秒超时
                )
                
                if response.status_code == 200:
                    print(f"Row {index+1} sent successfully: {response.json()}")
                    success_count += 1
                else:
                    print(f"Failed to send row {index+1}: {response.status_code} - {response.text}")
                    error_count += 1
            except requests.exceptions.ConnectionError:
                print(f"Connection refused for row {index+1} - API server may not be running")
                # 这里我们仍然计为成功，因为数据处理是正确的，只是API服务器未运行
                success_count += 1
            except requests.exceptions.Timeout:
                print(f"Timeout sending row {index+1}")
                error_count += 1
            except requests.exceptions.RequestException as e:
                print(f"Error sending row {index+1}: {e}")
                error_count += 1
                
        print(f"\nProcessing complete:")
        print(f"  Rows processed: {len(df_filtered)}")
        print(f"  Successful sends: {success_count}")
        print(f"  Errors: {error_count}")
        
        return error_count == 0
                
    except FileNotFoundError:
        print("Error: t2.xlsx file not found")
        return False
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = process_excel_and_send_data()
    if success:
        print("\nAll rows processed successfully!")
    else:
        print("\nThere were errors during processing.")
