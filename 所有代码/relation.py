import csv

# 节点数据CSV文件路径
nodes_file_path = 'neo4j_node.csv'
# 定义每个CSV文件中关系的最大数量
max_relations_per_file = 1000000

# 读取节点数据
nodes = {}
with open(nodes_file_path, mode='r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        nodes[row['_id:id']] = row

# 构建关系，并分批写入CSV文件
relations = []
relation_count = 0  # 计数器，用于跟踪当前处理的关系数量
relations_file_number = 1  # 用于生成每个CSV文件的编号

for node_id1, node1 in nodes.items():
    for node_id2, node2 in nodes.items():
        if node_id1 == node_id2:
            continue  # 忽略自身关系

        # 检查相同任务
        if node1['pipelineTag'] != 'unknown' and node1['pipelineTag'] == node2['pipelineTag']:
            relation_count += 1  # 更新关系计数器
            relations.append({':START_ID': node_id1, ':END_ID': node_id2, ':TYPE': 'same_task'})

        # 检查相同数据集
        if node1['datasets'] != 'unknown' and node1['datasets'] == node2['datasets']:
            relation_count += 1  # 更新关系计数器
            relations.append({':START_ID': node_id1, ':END_ID': node_id2, ':TYPE': 'same_datasets'})

        # 检查相同架构
        if node1['architectures'] != 'unknown' and node1['architectures'] == node2['architectures']:
            relation_count += 1  # 更新关系计数器
            relations.append({':START_ID': node_id1, ':END_ID': node_id2, ':TYPE': 'same_architectures'})

        if relations:
            if relation_count % max_relations_per_file == 0:
                # 写入当前批次的关系到CSV文件
                relations_file_path = f'relations/neo4j_relations_{relations_file_number}.csv'
                with open(relations_file_path, mode='w', newline='', encoding='utf-8') as outfile:
                    writer = csv.writer(outfile)
                    writer.writerow([':START_ID', ':END_ID', ':TYPE'])  # 写入标题行
                    for relation in relations:
                        writer.writerow([relation[':START_ID'], relation[':END_ID'], relation[':TYPE']])
                print(f'关系数据已写入CSV文件: {relations_file_path}')

                # 清空当前批次的关系列表，为下一个文件做准备
                relations.clear()
                relations_file_number += 1  # 更新文件编号
                relation_count = 0

# 如果最后一批关系未达到max_relations_per_file，也写入文件
if relations:
    relations_file_path = f'relations/neo4j_relations_{relations_file_number}.csv'
    with open(relations_file_path, mode='w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow([':START_ID', ':END_ID', ':TYPE'])  # 写入标题行
        for relation in relations:
            writer.writerow([relation[':START_ID'], relation[':END_ID'], relation[':TYPE']])
    print(f'剩余的关系数据已写入CSV文件: {relations_file_path}')

