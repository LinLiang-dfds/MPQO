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
import json

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
    elapsed_time = None   
    memory_usage = None   
    explain_output_buffers = ""
    # explain_output_timing = ""
    execution_plan = []

    while True:
        try:
            conn = psycopg2.connect(dbname='imdb', user='linliang', host='localhost', port=5434)
            cur = conn.cursor()
            
            cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
            cur.execute(f"SET enable_bao_selection TO {bao_select}")
            cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
            cur.execute("SET bao_num_arms TO 5")
            cur.execute("SET statement_timeout TO 120000")

            cur.execute(f"EXPLAIN ANALYZE {sql}")
            execution_plan_result = cur.fetchall()
            for row in execution_plan_result:
                execution_plan.append(row)

            cur.execute(f"EXPLAIN(ANALYZE,BUFFERS) {sql}")
            explain_result_buffers = cur.fetchall()
            for row in explain_result_buffers:
                explain_output_buffers += str(row) + "\n"
        
            #获取执行时间和内存使用
            memory_usage = extract_memroy_usage(explain_output_buffers)
            elapsed_time = extract_time_usage(execution_plan)
            
            conn.close()   
            break
        except:
            print("timeout")
            sleep(1)
            break

    return elapsed_time, memory_usage, json.dumps(execution_plan)

queries = read_file()
print("Read", len(queries), "queries.")
print("Using Bao:", USE_BAO)

print("Executing queries using PG optimizer for initial training")

#给pg作预训练
result_file_path = "/data1/linliang/Bao/BaoForPostgreSQL/MPQO_result/three_metric_analyze.txt"
with open(result_file_path, "w") as f:
    for fp, q in queries:
        pg_time_exe, pg_memory, pg_exe_plan = run_query(q, bao_reward=True)
        result_entry = f"SQL: {fp}\nTime: {pg_time_exe}\nMemory: {pg_memory}\nPlan: {pg_exe_plan}\n"
        f.write(result_entry)

print("Results written to file:", result_file_path)
