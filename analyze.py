import json
import matplotlib.pyplot as plt
import math


def load_json(path):
    with open(path, 'r') as json_file:
        json_obj = json.load(json_file)
    return json_obj 

def get_pg_plan(pg_plan_path):
    plan = load_json(pg_plan_path)
    test_plan = plan[int(0.8 * len(plan)):]
    return test_plan

def find_shared_hit_blocks(data,res=[]):
    if "Shared Hit Blocks" in data:
        res.append(data["Shared Hit Blocks"])
        
    if "Plans" in data:
        for plan in data["Plans"]:
            find_shared_hit_blocks(plan)
    
    return res

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


#
def cal_GMRL(pg_time, bao_time):
    res = 1
    n = min(len(pg_time), len(bao_time))
    for i in range(n):
        if(pg_time[i] == 0):break
        
        res *= float(bao_time[i]) / float(pg_time[i])
        # res1 = math.pow(res, 1 / (i + 1))
        # print(f'epoch:{i}, bao_time:{bao_time[i]}, pg_time:{pg_time[i]}, res:{res1}')
    
    res = math.pow(res, 1 / n)
    return res


pg_plan_path = '/data1/linliang/Bao/BaoForPostgreSQL/data/pg_plan_13k.json'
bao_plan_path = '/data1/linliang/Bao/BaoForPostgreSQL/data/bao_test_13k.json'

pg_plan = get_pg_plan(pg_plan_path)
pg_name, pg_time, pg_mem = get_pg_plan_status(pg_plan)
bao_test_name, bao_test_time, bao_test_mem = get_bao_plan_status(bao_plan_path)
print('整个测试集的大小为:{}'.format(len(bao_test_time)))
# print(len(pg_time))
print(max(bao_test_mem))


# gmrl = cal_GMRL(pg_time, bao_test_time)   # batch_size=16, GMRL=1.1813928290613602      batch_size=32,  GMRL=1.534698108330786       bao_train GMRL=1.1171283374533059       13k GMRL:1.2173214272262003
# print(f'GMRL: {gmrl}')                    # batch_size=64, GMRL=1.5879460768326332      batch_size=128, GMRL=1.460808851035837       batch_size=256, GMRL=1.230545657557375


count = 0
count1 = 0
count2 = 0
for i in range(len(bao_test_time)):
    if((float(bao_test_time[i]) < float(pg_time[i])) and (float(bao_test_mem[i]) > float(pg_mem[i]))): count += 1
    if(float(bao_test_time[i]) < float(pg_time[i])): count1 += 1
    if(bao_test_mem[i] == 0 and pg_time[i] == 0): count2 += 1
print("Bao time < PG time,Bao mem > PG mem number:{}".format(count))
print("Bao time < PG time number:{}".format(count1)) 
print("Bao time = PG time = 0,number:{}".format(count2))