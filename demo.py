import psycopg2
import os
import sys
import random
from time import time, sleep
import traceback
import sys
import io
# 设置默认编码为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

USE_BAO = True
PG_CONNECTION_STR = "dbname=imdb user=linliang host=localhost"

# https://stackoverflow.com/questions/312443/
#输入一个列表和需要分割成多少块的数量，产生列表的n个这些子块
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n] 


def run_query(sql, bao_select=False, bao_reward=False):
    elapsed_time = None   #获取的时间
    memory_usage = None   #获取的内存
    while True:
        try:
            conn = psycopg2.connect(dbname='imdb', user='linliang', host='localhost', port=5434)
            cur = conn.cursor()

            # Execute EXPLAIN ANALYZE
            cur.execute(f"EXPLAIN ANALYZE {sql}")
            explain_result = cur.fetchall()
            for row in explain_result:
                print(row)

            # Execute the query
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 300000")
            cur.execute(sql)
            result = cur.fetchall()

            # Check pg_stat_statements for query statistics
            cur.execute("SELECT * FROM pg_stat_statements WHERE query = %s", (sql,))
            stats = cur.fetchall()
            for stat in stats:
                print(stat)
                # Assuming that you want the total memory usage, you can sum up relevant fields
                memory_usage = stat['shared_blks_hit'] + stat['shared_blks_read'] + stat['shared_blks_dirtied'] + stat['shared_blks_written'] + stat['local_blks_hit'] + \
                    stat['local_blks_read'] + stat['local_blks_dirtied'] + stat['local_blks_written'] + stat['temp_blks_read'] + stat['temp_blks_written']
                elapsed_time = stat['total_time']

            conn.close()   
            break
        except Exception as e:
            sleep(1)
            print(e)
            break

    return elapsed_time, memory_usage

#sys.argv[1:]是Python中的一个列表，包含命令行参数，sys.argv[0]表示脚本本身的名称        
query_paths = sys.argv[1:]
queries = []
for fp in query_paths:
    with open(fp) as f:
        query = f.read() #读取sql
    queries.append((fp, query))  #将文件路径和查询内容的元组添加到列表中
print("Read", len(queries), "queries.")
print("Using Bao:", USE_BAO)

training_set = queries[:28]  #前70%的数据集作为训练集
testing_set = queries[28:]   #后30%的数据集作为测试集


print("Executing queries using PG optimizer for initial training")

for fp, q in training_set:
    pg_time, pg_memory = run_query(q, bao_reward=True)
    print("x", "x", fp, pg_time, pg_memory, "PG", flush=True)

for c_idx, chunk in enumerate(testing_set):
    if USE_BAO:
        os.system("cd bao_server && python3 baoctl.py --retrain")
        os.system("sync")
  
    for q_idx, (fp, q) in enumerate(chunk):
        q_time, pg_memory = run_query(q, bao_reward=USE_BAO, bao_select=USE_BAO)
        print(c_idx, q_idx, fp, q_time, pg_memory, flush=True)
