import json
import math


def load_json(path):
    with open(path, 'r') as json_file:
        json_obj = json.load(json_file)
    return json_obj 

def get_pg_plan(pg_plan_path):
    plan = load_json(pg_plan_path)
    test_plan = plan[int(0.8 * len(plan)):]
    return test_plan

def get_pg_plan_status(plan):
    plan_name = []
    plans = []
    time = []
    mem_res = []
    for i in range(len(plan)):
        plan_name.append(plan[i]['Query'])
        if plan[i]['Plan'] == 'timeout':
            plans.append('None')
            time.append(120000)
            mem_res.append(0)
        else:
            mem = []
            plan_tmp = plan[i]['Plan'][0][0][0]
            plans.append(plan_tmp)
            time.append(plan_tmp['Execution Time'])
            mem.append(plan_tmp['Plan']['Shared Hit Blocks'])
            plan_tmp = plan_tmp['Plan']
            mem.append(find_shared_hit_blocks(plan_tmp,mem))
            mem_num = [int(x) for x in mem if isinstance(x, (int, float))]
            mem_res.append(max(mem_num) * 8 / 1024)
            
    return plan_name, time, mem_res

def get_bao_plan_status(plan_path):
    plan = load_json(plan_path)
    plan_name = []
    plans = []
    time = []
    mem_res = []
    for i in range(len(plan)):
        plan_name.append(plan[i][0]['Query'])
        if plan[i][0]['Plan'] == 'timeout':
            plans.append('None')
            time.append(240000)
            mem_res.append(0)
        else:
            mem = []
            plan_tmp = plan[i][0]['Plan'][0][0][1]
            plans.append(plan_tmp)
            time.append(plan_tmp['Execution Time'])
            mem.append(plan_tmp['Plan']['Shared Hit Blocks'])
            plan_tmp = plan_tmp['Plan']
            mem.append(find_shared_hit_blocks(plan_tmp,mem))
            mem_num = [int(x) for x in mem if isinstance(x, (int, float))]
            mem_res.append(max(mem_num) * 8 / 1024)
            
    return plan_name, time, mem_res



#
def find_shared_hit_blocks(data,res=[]):
    # 检查当前节点是否包含 "Shared Hit Blocks" 键
    if "Shared Hit Blocks" in data:
        # 如果包含，则将其值添加到结果列表中
        res.append(data["Shared Hit Blocks"])
    
    # 递归地处理子节点
    if "Plans" in data:
        for plan in data["Plans"]:
            find_shared_hit_blocks(plan)
    
    return res

pg_plan_path = '/data1/linliang/Bao/BaoForPostgreSQL/data/pg_plan.json'
bao_plan_path = '/data1/linliang/Bao/BaoForPostgreSQL/data/bao_test_256.json'

pg_plan = get_pg_plan(pg_plan_path)
pg_name, pg_time, pg_mem = get_pg_plan_status(pg_plan)
bao_test_name, bao_test_time, bao_test_mem = get_bao_plan_status(bao_plan_path)
print(len(pg_mem))
print(len(pg_time))

for i in range(len(pg_mem)):
    print(pg_mem[i])

