import matplotlib.pyplot as plt

# 读取文本文件并解析数据
data_file = '/data1/linliang/Bao/BaoForPostgreSQL/MPQO_result/pg_time_mem_result.txt'
with open(data_file, 'r') as file:
    lines = file.readlines()

queries = []
memory = []
execution_time = []

for line in lines:
    parts = line.split()
    query_name = parts[3].split('/')[-1].split('.')[0]  # 获取SQL查询的名称
    memory_size = float(parts[-1])
    exec_time = float(parts[-2])

    if memory_size != 'None' and exec_time != 'None':
        queries.append(query_name)
        memory.append(memory_size)
        execution_time.append(exec_time)

# 绘制散点图
plt.figure(figsize=(10, 6))
for i in range(len(queries)):
    plt.scatter(memory[i], execution_time[i], label=queries[i])
    plt.text(memory[i], execution_time[i], queries[i], fontsize=9)  # 在每个数据点上标记SQL名称

plt.xlabel('Memory Size (MiB)')
plt.ylabel('Execution Time (ms)')
plt.title('Postgre Memory Size vs. Execution Time for SQL Queries')
plt.legend(loc = 'center right', title = 'sql')
plt.grid(True)
plt.show()
plt.savefig('MPQO_result/pg_time_mem_result.png')