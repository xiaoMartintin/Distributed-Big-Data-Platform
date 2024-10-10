from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col, regexp_replace

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("CardData Datasets Count") \
    .getOrCreate()

# 读取 CSV 文件并创建 DataFrame
df = spark.read.csv("./cleaned_data.csv", header=True)
# 过滤掉值为"unknown"的项
df = df.filter(col("datasets") != "unknown")

# 去除每个元素周围的单引号
df = df.withColumn("datasets", regexp_replace(col("datasets"), "[\[\]']", ""))

# 将字符串类型的列拆分成数组
df = df.withColumn("datasets", split(col("datasets"), ","))

# 展开数组列并且统计每个元素的个数
dataset_counts = df.select(explode(col("datasets")).alias("dataset")) \
                  .groupBy("dataset").count()

# 按照 count 降序排序
dataset_counts_sorted = dataset_counts.orderBy(col("count").desc())

# 显示前 20 条结果
dataset_counts_sorted.show(20)

with open('./dataset_counts_sorted2.csv', mode='w', newline='') as file:
    writer = csv.writer(file)
    # 写入表头
    writer.writerow(['dataset', 'count'])

    # 循环访问前20项并写入CSV文件
    for row in dataset_counts_sorted.head(20):
        dataset = row["dataset"]
        count = row["count"]
        writer.writerow([dataset, count])


# 关闭 SparkSession
spark.stop()
