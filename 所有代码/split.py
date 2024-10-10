import json

# 读取 JSON 文件
with open('hf_metadata.json', 'r') as file:
    data = json.load(file)

# 确定拆分方式：按行拆分
num_lines = len(data)
lines_per_file = num_lines // 3

# 执行拆分操作
for i in range(3):
    start_index = i * lines_per_file
    end_index = (i + 1) * lines_per_file if i < 2 else None
    subset_data = data[start_index:end_index]

    # 保存拆分结果
    with open(f'output_{i}.json', 'w') as output_file:
        json.dump(subset_data, output_file, indent=4)

