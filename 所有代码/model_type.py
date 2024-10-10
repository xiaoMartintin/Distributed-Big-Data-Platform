from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import csv

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("Model Type Count") \
    .getOrCreate()

# 读取 CSV 文件并创建 DataFrame
df = spark.read.csv("./cleaned_data.csv", header=True)

# 统计每个 config_model_type 对应的 model 个数
type_counts = df.groupBy("model_type").count()

# 按照 count 降序排序
type_counts_sorted = type_counts.orderBy(col("count").desc())

# 显示前 20 条结果
type_counts_sorted.show(20)

with open('./model_type_counts2.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # 写入表头
    writer.writerow(['model_type', 'count'])

    # 循环访问前20项并写入CSV文件
    for row in type_counts_sorted.head(20):
        model_type = row["model_type"]
        count = row["count"]
        writer.writerow([model_type, count])

# 关闭 SparkSession
spark.stop()