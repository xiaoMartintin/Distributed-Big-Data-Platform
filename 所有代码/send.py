from kafka import KafkaProducer
import json
import time

# 创建 Kafka 生产者
producer = KafkaProducer(bootstrap_servers='dip-3:9092')

# 指定 JSON 文件路径
json_file_path = 'hf_metadata.json'

# 读取 JSON 文件
with open(json_file_path, 'r') as file:
    json_data = json.load(file)

# 计算每次发送的消息数量
batch_size = len(json_data) // 10

# 将 JSON 数据发送到 Kafka 主题
# 在发送前，你可能需要将 JSON 数据编码为字节串
for i in range(0, len(json_data), batch_size):
    batch = json_data[i:i+batch_size]
    for data in batch:
        producer.send('quickstart', json.dumps(data).encode('utf-8'))
    producer.flush()  # 确保所有消息被发送
    time.sleep(20) #20 秒

# 关闭生产者连接
producer.close()
