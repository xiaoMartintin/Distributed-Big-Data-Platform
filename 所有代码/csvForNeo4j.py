import csv

# 原始CSV文件路径
input_file_path = 'cleaned_data.csv'
# 新的CSV文件路径，包含自增ID列
output_file_path = 'neo4j_node.csv'

# 打开原始CSV文件，并创建一个新的文件用于写入
with open(input_file_path, mode='r', newline='', encoding='utf-8') as infile, \
        open(output_file_path, mode='w', newline='', encoding='utf-8') as outfile:
    # 创建CSV读写器
    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    # 写入标题行，添加一个额外的"id"列
    headers = next(reader)
    headers.insert(0, '_id:id')  # 在第一列前插入"id"
    # 写入标题行
    writer.writerow(headers)

    # 遍历每一行数据，添加自增ID
    _id = 1  # 开始的ID值
    for row in reader:
        newRow = [_id] + row  # 将ID添加到行的开始
        # 写入新的行
        writer.writerow(newRow)
        _id += 1  # 增加ID的值

print(f'ID列已添加到新的CSV文件: {output_file_path}')

