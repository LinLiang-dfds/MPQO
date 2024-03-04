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
    max_memory_usage = 0
    for line in explain_output.split('\n'):
        if 'Buffers' in line:
            match = re.search(r'shared hit=(\d+)',line)
            if match:
                memory_usage = int(match.group(1)) * 8 / 1024
                if memory_usage > max_memory_usage:
                    max_memory_usage = memory_usage
    return max_memory_usage

explain_out_t1 = """Limit  (cost=312472.59..312589.27 rows=1000 width=16) (actual time=314.798..316.400 rows=1000 loops=1)
   Buffers: shared hit=54173
   ->  Gather Merge  (cost=312472.59..1186362.74 rows=7489964 width=16) (actual time=314.794..316.358 rows=1000 loops=1)
         Workers Planned: 2
         Workers Launched: 2
         Buffers: shared hit=54173
         ->  Sort  (cost=311472.57..320835.02 rows=3744982 width=16) (actual time=309.456..309.472 rows=784 loops=3)
               Sort Key: num
               Sort Method: top-N heapsort  Memory: 128kB
               Buffers: shared hit=54173
               Worker 0:  Sort Method: top-N heapsort  Memory: 127kB
               Worker 1:  Sort Method: top-N heapsort  Memory: 128kB
               ->  Parallel Seq Scan on t1  (cost=0.00..106139.24 rows=3744982 width=16) (actual time=0.019..193.371 rows=3000173 loops=3)
                     Filter: (num > 10000)
                     Rows Removed by Filter: 333161
                     Buffers: shared hit=54055
 Planning Time: 0.212 ms
 Execution Time: 316.461 ms
(18 rows) """

explain_out_t2 = """ Limit  (cost=0.43..24.79 rows=1000 width=16) (actual time=0.044..3.089 rows=1000 loops=1)
   Buffers: shared hit=1003
   ->  Index Scan using i_t2_num on t2  (cost=0.43..219996.68 rows=9034644 width=16) (actual time=0.042..2.990 rows=1000 loops=1)
         Index Cond: (num > 10000)
         Buffers: shared hit=1003
 Planning Time: 0.167 ms
 Execution Time: 3.172 ms
(7 rows)"""
print(extract_memroy_usage(explain_out_t1))
print('===============================')
print(extract_memroy_usage(explain_out_t2))
