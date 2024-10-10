## 数据分析代码

send.py : 从json文件中读取数据并发送到Kafka指定的topic中

split.py : 将json文件进行拆分输出

cleanData.py : 从kafka的特定topic中读取数据，检查其模型名称和下载量等重要信息是否为空，若不为空则结构化存储到neo4j数据库中

sparkGraphXAnalyse.py : 从neo4j中，根据关联拿取数据并按照发布作者划分的模型下载量排⾏、按模型家族划分的模型数量排⾏、按照数据集划分的模型数量和下载量排⾏

jsonToCsv.py : 将json文件转化为格式化的csv文件，便于从hdfs导入hive进行持久化

csvForNeo4j.py : 在csv文件添加首列，列名为id，内容为自增的id，方便作为neo4j的label

relation.py : 在csv文件中,根据同数据集，同家族，同模型关系，提取关联关系，分多次存入不同的csv文件中

meger_relations.py : 将处理好的多个relation关系csv文件合并为一个，便于上传到neo4j进行持久化
