import matplotlib.pyplot as plt

# Function to read data from the text file
def read_data(filename):
    data = {}
    with open(filename, 'r') as file:
        for line in file:
            parts = line.strip().split()
            sql_name = parts[0].split('.')[0]
            pg_time = float(parts[1])
            pg_mem = float(parts[2])
            bao_time = float(parts[3])
            bao_mem = float(parts[4])
            data[sql_name] = {'pg': (pg_mem, pg_time), 'bao': (bao_mem, bao_time)}
    return data

# Function to create plot
def create_plot(data):
    plt.figure(figsize=(10, 6))
    for sql_name, values in data.items():
        pg_mem, pg_time = values['pg']
        bao_mem, bao_time = values['bao']
        plt.scatter(pg_mem, pg_time, color='blue', label='pg_' + sql_name)
        plt.scatter(bao_mem, bao_time, color='orange', label='bao_' + sql_name)
        plt.text(pg_mem, pg_time, sql_name, fontsize=8, ha='right', va='bottom')
        plt.text(bao_mem, bao_time, sql_name, fontsize=8, ha='right', va='bottom')
    plt.xlabel('Memory Size(MiB)')
    plt.ylabel('Execution Time(ms)')
    plt.title('Execution Time vs Memory Size for SQL Queries')
    plt.grid(True)
    plt.show()
    plt.savefig('/data1/linliang/Bao/BaoForPostgreSQL/MPQO_result/pg_bao_compare.png')

# Main function
def main():
    filename = '/data1/linliang/Bao/BaoForPostgreSQL/MPQO_result/pg_result.txt'  # Provide the correct filename here
    data = read_data(filename)
    create_plot(data)

if __name__ == "__main__":
    main()
