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

#计算实际的执行内存
def extract_memroy_usage(explain_output):
    memory_usage = 0

    for line in explain_output.split('\n'):
        if 'Buffers' in line:
            match = re.search(r'shared hit=(\d+)',line)
            if match:
                memory_usage += int(match.group(1)) * 8 / 1024
            break
    return memory_usage

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
            cur.execute("SET statement_timeout TO 300000")
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

train_memory = []
train_time = []
test_memory = []
test_time = []

#sys.argv[1:]是Python中的一个列表，包含命令行参数，sys.argv[0]表示脚本本身的名称        
query_paths = sys.argv[1:]
queries = []
for fp in query_paths:
    with open(fp) as f:
        query = f.read() #读取sql
    queries.append((fp, query))  #将文件路径和查询内容的元组添加到列表中
print("Read", len(queries), "queries.")
print("Using Bao:", USE_BAO)

print('pg start to execute...')
#给pg作预训练
for fp, q in queries:
    pg_time, pg_memory = run_query(q, bao_reward=True)
    print("x", "x", time(), fp, pg_time, pg_memory, "PG", flush=True)

