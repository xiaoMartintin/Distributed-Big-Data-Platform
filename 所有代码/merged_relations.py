import csv
import os

# CSV 文件的存放路径
folder_path = 'relations'
# 新的 CSV 文件名
new_csv_file = 'merged_neo4j_relations.csv'

# 存储所有 CSV 文件的完整路径
csv_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.startswith('neo4j_relations') and f.endswith('.csv')]

# 写入新的 CSV 文件的标题行(只写一次)
with open(new_csv_file, mode='w', newline='', encoding='utf-8') as outfile:
    writer = csv.writer(outfile)

    # 假设所有 CSV 文件的标题行都是相同的，我们只从第一个文件中获取标题行并写入
    if csv_files:
        with open(csv_files[0], mode='r', newline='', encoding='utf-8') as first_file:
            reader = csv.reader(first_file)
            first_row = next(reader)  # 读取标题行
            writer.writerow(first_row)  # 写入标题行

    # 遍历所有 CSV 文件，并将内容追加到新的 CSV 文件中
for csv_file in csv_files:
    with open(csv_file, mode='r', newline='', encoding='utf-8') as infile:
        reader = csv.reader(infile)
        next(reader)  # 跳过标题行，因为我们已经写入了标题行
        with open(new_csv_file, mode='a', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            for row in reader:
                writer.writerow(row)
    print(f'已合并文件: {csv_file}')
print(f'所有文件已合并到')
