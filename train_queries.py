import psycopg2
import os
import sys
import json

USE_BAO = True
USE_PG = False


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
     

    def execute(self, query, bao_select, bao_reward):
        self.cur.execute(f"SET enable_bao TO {bao_select or bao_reward}")
        self.cur.execute(f"SET enable_bao_selection TO {bao_select}")
        self.cur.execute(f"SET enable_bao_rewards TO {bao_reward}")
        self.cur.execute("SET bao_num_arms TO 5")
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

# folder_path = sys.argv[1]
# def read_file():
#     queries = []

#     for root, dirs, files in os.walk(folder_path):
#         for file in files:
#             if file.endswith(".sql"):
#                 file_path = os.path.join(root, file)
#                 with open(file_path) as f:
#                     query = f.read()
#                 queries.append((file, query))
#     return queries

def load_json(path):
    with open(path, 'r') as json_file:
        json_obj = json.load(json_file)
    return json_obj 

def train_and_test_set_split(queries, ratio):   
    training_set = queries[:int(ratio * len(queries))]
    testing_set = queries[int(ratio * len(queries)):]

    return training_set, testing_set

def run_query(sql, bao_select, bao_reward):
    plans = []
    try:
        db = Database('imdb')
        db.set_timeout(240000)
        plan = db.execute('EXPLAIN (ANALYZE true, FORMAT JSON, TIMING, BUFFERS, VERBOSE false)' + sql,bao_select,bao_reward)
        plans.append({'Query': fp, 'Plan': plan})
        db.close()
    except:
        plans.append({'Query': fp, 'Plan': 'timeout'})

    return plans

# queries = read_file()   #获取查询
# training_set, testing_set = train_and_test_set_split(queries, 0.8)
# print("Train", len(training_set), "queries.")
# print("Test", len(testing_set), "queries.")
# print("Using Bao:", USE_BAO)

# train_plans = []
# for (fp, q) in training_set:   #用bao进行训练
#     train_plans.append(run_query(q, bao_select=USE_PG, bao_reward=USE_BAO))

if USE_BAO:
    os.system("cd bao_server && /data1/linliang/anaconda3/envs/py310/bin/python3 baoctl.py --train /data1/linliang/Bao/BaoForPostgreSQL/bao_server/bao_train_model_256")
    os.system("sync")

# result_file_path_train = '/data1/linliang/Bao/BaoForPostgreSQL/data/bao_train.json'
# with open(result_file_path_train, 'w') as f:
#     json.dump(train_plans, f)

# print("Training Results have already been written to file:", result_file_path_train)

#测试阶段
# test_plans = []
# if USE_BAO:
#     os.system("cd bao_server && /data1/linliang/anaconda3/envs/py310/bin/python3 baoctl.py --load /data1/linliang/Bao/BaoForPostgreSQL/bao_server/bao_train_model")
#     os.system("sync")

# for (fp, q) in testing_set:
#     test_plans.append(run_query(q, bao_select=USE_BAO, bao_reward=USE_PG))

# print(len(test_plans))
     
# result_file_path_test = '/data1/linliang/Bao/BaoForPostgreSQL/data/bao_test.json'
# with open(result_file_path_test, 'w') as f:
#     json.dump(test_plans, f)

# print("Testing Results have already been written to file:", result_file_path_test)
