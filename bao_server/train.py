import storage
import model
import os
import shutil
import reg_blocker
import json

class BaoTrainingException(Exception): #自定义异常类
    pass

def load_json(fn):
    with open(fn,'r') as f:
        json_obj = json.load(f)
    return json_obj

def train_and_swap(fn, old, tmp, verbose=False): #训练模型并在一定条件时，进行模型替换
    if os.path.exists(fn): # 如果指定路径（fn）已存在，加载旧模型
        old_model = model.BaoRegression(have_cache_data=True)
        old_model.load(fn)
    else:
        old_model = None

    # 训练并保存新模型
    new_model = train_and_save_model(tmp, verbose=verbose)
    max_retries = 5
    current_retry = 1
    # 当新模型不能替代旧模型时，尝试重新训练
    while not reg_blocker.should_replace_model(old_model, new_model):
        if current_retry >= max_retries == 0:
            print("Could not train model with better regression profile.")
            return
        
        print("New model rejected when compared with old model. "
              + "Trying to retrain with emphasis on regressions.")
        print("Retry #", current_retry)
        # 重新训练并保存新模型
        new_model = train_and_save_model(tmp, verbose=verbose,
                                         emphasize_experiments=current_retry)
        current_retry += 1

    if os.path.exists(fn):
        shutil.rmtree(old, ignore_errors=True)
        os.rename(fn, old)
    os.rename(tmp, fn)

def train_and_test_set_split(queries, ratio):   
    training_set = queries[:int(ratio * len(queries))]
    testing_set = queries[int(ratio * len(queries)):]

    return training_set, testing_set

def get_plan_status(fn):
    plan = load_json(fn)
    plan, plan_test = train_and_test_set_split(plan,0.8)
    plan_name = []
    plans = []
    time = []
    mem = []
    for i in range(len(plan)):
        plan_name.append(plan[i]['Query'])
        if plan[i]['Plan'] != 'timeout':
            plan_tmp = plan[i]['Plan'][0][0][0]
            plans.append(plan_tmp)
            time.append(plan_tmp['Execution Time'])
            mem.append(plan_tmp['Plan']['Shared Hit Blocks'] * 8 / 1024)
    return plans, time

plan_path = '/data1/linliang/Bao/BaoForPostgreSQL/data/pg_plan.json'
def train_and_save_model(fn, verbose=True, emphasize_experiments=0):
    # all_experience = storage.experience()

    # for _ in range(emphasize_experiments):
    #     all_experience.extend(storage.experiment_experience())
    
    # x = [i[0] for i in all_experience]    # plan
    # y = [i[1] for i in all_experience]    # reward
    
    # if not all_experience:
    #     raise BaoTrainingException("Cannot train a Bao model with no experience")
    
    # if len(all_experience) < 20:
    #     print("Warning: trying to train a Bao model with fewer than 20 datapoints.")
    x, y = get_plan_status(plan_path)

    reg = model.BaoRegression(have_cache_data=False, verbose=verbose)
    # reg = model.BaoRegression(have_cache_data=True, verbose=verbose)
    reg.fit(x, y)
    reg.save(fn)
    return reg


if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: train.py MODEL_FILE")
        exit(-1)
    train_and_save_model(sys.argv[1])

    print("Model saved, attempting load...")
    reg = model.BaoRegression(have_cache_data=True)
    reg.load(sys.argv[1])

