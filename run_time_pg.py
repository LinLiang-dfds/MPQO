import psycopg2
import os
import sys
from time import time, sleep
import sys
import io
import json
from tqdm import tqdm

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

folder_path = sys.argv[1]
def read_file():
    queries = []

    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                with open(file_path) as f:
                    query = f.read()
                queries.append((file, query))
    return queries

class Database:
    def __init__(self, database_name):
        self.database_name = database_name
        self.conn = psycopg2.connect(
            host = "localhost",
            dbname = database_name,
            user = "linliang",
            port = 5434,
        )
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        records = self.cur.fetchall()
        # self.conn.commit()
        return records

    def set_timeout(self, timeout):
        self.cur.execute(f"SET statement_timeout = {timeout};")
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()
        
queries = read_file()
print("Read", len(queries), "queries.")

plans = []

for (fp, sql) in tqdm(queries):
    try:
        db = Database('imdb')
        db.set_timeout(120000)
        plan = db.execute('EXPLAIN (ANALYZE true, FORMAT json, TIMING, BUFFERS, verbose false) ' + sql)
        db.execute('SELECT pg_stat_reset();')
        plans.append({'Query': fp, 'Plan': plan})
        db.close()
    except:
        print('timeout')
        plans.append({'Query': fp, 'Plan': 'timeout'})


result_file_path = '/data1/linliang/Bao/BaoForPostgreSQL/data/pg_plan_sq_pg_stat.json'
with open(result_file_path, 'w') as f:
    json.dump(plans, f)

print("Results have already been written to file:", result_file_path)