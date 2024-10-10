import csv
import json
from tqdm import tqdm

# 读取JSON文件
with open('./hf_metadata.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# 设置CSV文件名
csv_file = 'cleaned_data.csv'

# 设置计数器以跟踪缺失数据的数量
missing_count = 0

# 清洗数据并写入CSV文件
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    fieldnames = ['modelId', 'lastModified', 'pipelineTag', 'author', 'likes', 'downloads', 'model_type', 'datasets', 'library_name', 'architectures']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for item in tqdm(data, desc="Cleaning data"):  # tqdm用于显示进度条
        config = item.get('config', {})
        card_data = item.get('cardData', {})

        # 检查config和card_data是否为字典类型，如果不是则设置为{}
        if not isinstance(config, dict):
            config = {}
        if not isinstance(card_data, dict):
            card_data = {}
        cleaned_item = {
            'modelId': item.get('modelId', None),
            'lastModified': item.get('lastModified', 'unknown'),
            'pipelineTag': item.get('pipeline_tag', None),
            'author': item.get('author', 'unknown'),  # 如果author缺失，则设置为'unknown'
            'likes': item.get('likes', 0),
            'downloads': item.get('downloads', None),
            'model_type': config.get('model_type', None),
            'datasets': card_data.get('datasets', 'unknown'),
            'library_name': item.get('library_name', 'unknown'),
            'architectures': config.get('architectures', 'unknown')
        }

        # 检查是否有缺失数据
        if None in cleaned_item.values():
            missing_count += 1
        else:
            writer.writerow(cleaned_item)

print(f"数据清洗完成，共处理了{len(data)}项数据，其中有{missing_count}项数据缺失，已从清洗后的数据中去除。")
