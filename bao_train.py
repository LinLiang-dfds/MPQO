import psycopg2
import os
import sys
import random
from time import time, sleep
import traceback
import sys
import io
import numpy as np
import re #处理正则表达式
import matplotlib.pyplot as plt
import pandas as pd
# 设置默认编码为 UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

USE_BAO = True
PG_CONNECTION_STR = "dbname=imdb user=linliang host=localhost"

def read_file():
    folder_path = sys.argv[1]
    queries = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                with open(file_path) as f:
                    query = f.read()
                queries.append((file, query))
    return queries


def split_train_test(data_list,train_ratio):
    np.random.seed(42)
    #对数组进行随机排序
    shuffled_indices = np.random.permutation(len(data_list))
    #test_ratio为测试机所占的百分比
    train_set_size = int(len(data_list) * train_ratio)
    train_indices = shuffled_indices[:train_set_size]
    test_indices = shuffled_indices[train_set_size:]
    #将列表转换为Pandas DataFrame对象
    
    # 根据索引划分数据集
    training_set = [data_list[i] for i in train_indices]
    testing_set = [data_list[i] for i in test_indices]
    #iloc选择参数序列中的对应的行
    return training_set,testing_set

#获取实际的执行时间
def extract_time_usage(explain_output):
   time_usage = 0
   for line in explain_output.split('\n'):
        if 'Planning Time' in line:
            match = re.search(r'Planning Time: (\d+\.\d+)', line)
            if match:
                time_usage += float(match.group(1))
        if 'Execution Time' in line:
            match = re.search(r'Execution Time: (\d+\.\d+)', line)
            if match:
                time_usage += float(match.group(1))
   return time_usage

#计算实际的执行内存
def extract_memroy_usage(explain_output):
    memory_usage = 0
    max_memory_usage = 0
    for line in explain_output.split('\n'):
        if 'Buffers' in line:
            match = re.search(r'shared hit=(\d+)',line)
            if match:
                memory_usage = int(match.group(1)) * 8 / 1024
                if memory_usage > max_memory_usage:
                    max_memory_usage = memory_usage
    return max_memory_usage

def run_query(sql, bao_select=False, bao_reward=False):
    elapsed_time = None   #获取的时间 
    memory_usage = None   #获取的内存
    explain_output_buffers = ""
    explain_output_timing = ""

    while True:
        try:
            conn = psycopg2.connect(dbname='imdb', user='linliang', host='localhost', port=5434)
            cur = conn.cursor()

            # Execute EXPLAIN ANALYZE(BUFFERS)
            cur.execute(f"EXPLAIN(ANALYZE,BUFFERS) {sql}")
            explain_result_buffers = cur.fetchall()
            for row in explain_result_buffers:
                explain_output_buffers += str(row) + "\n"

             # Execute EXPLAIN ANALYZE (TIMING)
            cur.execute(f"EXPLAIN(ANALYZE,TIMING) {sql}")
            explain_result_timing = cur.fetchall()
            for row in explain_result_timing:
                explain_output_timing += str(row) + "\n"

            # Execute the query
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 60000")  
            cur.execute(sql)
            result = cur.fetchall()
            
            #获取执行时间和内存使用
            memory_usage = extract_memroy_usage(explain_output_buffers)
            elapsed_time = extract_time_usage(explain_output_timing)
          
            conn.close()   
            break
        except Exception as e:
            sleep(1)
            print(e)
            break

    return elapsed_time, memory_usage

def test_queries(testing_set, test_time):
    for c_idx, chunk in enumerate(testing_set):
        for q_idx, (fp,q) in enumerate(chunk):
            q_time = run_query(q,bao_reward=USE_BAO, bao_select=USE_BAO)
            test_time.append(q_time)
            print("Testing Query:",c_idx, q_idx, fp, q_time, flush = True)


queries = read_file()
print("Read", len(queries), "queries.")
print("Using Bao:", USE_BAO)

pg_time_train = []
pg_mem_train = []
train_memory = []
train_time = []
test_memory = []
test_time = []

print("Executing queries using PG optimizer for initial training")
#给pg作预训练
for fp, q in queries:
    pg_time, pg_memory = run_query(q, bao_reward=True)
    pg_time_train.append((fp, pg_time))
    pg_mem_train.append((fp, pg_memory))
    print("PG", fp, pg_time, pg_memory, flush=True)

pg_time_train_dict = dict(pg_time_train)
pg_mem_train_dict = dict(pg_mem_train)

training_set,testing_set = split_train_test(queries,0.8)  #前80%的数据集作为训练集,后20%的数据集作为测试集

#Bao训练
for chunk in enumerate(training_set):
    if USE_BAO:
        os.system("cd bao_server && /data1/linliang/anaconda3/envs/py310/bin/python3 baoctl.py --retrain")
        os.system("sync")
        q_time,q_memory = run_query(chunk[1], bao_reward=USE_BAO, bao_select=USE_BAO)
        train_memory.append(q_memory)
        train_time.append(q_time)
        print("Bao Training Query:", chunk[0], q_time, q_memory, flush=True)

print('\n')

#Bao测试
for chunk in enumerate(testing_set):
        q_time, q_memory = run_query(chunk[1],bao_reward=USE_BAO, bao_select=USE_BAO)
        test_time.append((chunk[0], q_time))
        test_memory.append((chunk[0], q_memory))
        print("Bao Testing Query:", chunk[0], q_time, q_memory, flush = True)

test_time_dict = dict(test_time)
test_memory_dict = dict(test_memory)

matched_pg_time = []
matched_pg_memory = []

for chunk in testing_set:
    for fp, q in chunk:
        if fp in pg_time_train_dict:
            pg_time = pg_time_train_dict[fp]
            test_time_match = test_time_dict[fp]
            matched_pg_time.append((fp, pg_time,test_time_match))

        if fp in pg_mem_train_dict:
            pg_memory = pg_mem_train_dict[fp]
            test_memory_match = test_memory_dict[fp]
            matched_pg_memory.append((fp, pg_memory,test_memory_match))

print('PG and Bao Execution Time')
for i in range(len(testing_set)):
    print(matched_pg_time[i])

print('PG and Bao Execution Memory')
for i in range(len(testing_set)):
    print(matched_pg_memory)