from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from graphframes import GraphFrame
from neo4j import GraphDatabase

# Connect to Neo4j
neo4j_url = "bolt://dip-3:7687"
neo4j_user = "neo4j"
neo4j_password = "neo4j123"

driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

# Retrieve data from Neo4j
def fetch_data_from_neo4j(driver):
    with driver.session() as session:
        result = session.run("MATCH (n)-[r]->(m) RETURN n, r, m")
        nodes = []
        relationships = []
        for record in result:
            nodes.append(record['n'])
            nodes.append(record['m'])
            relationships.append(record['r'])
        return {'nodes': nodes, 'relationships': relationships}

# Create SparkContext
sc = SparkContext("local", "Neo4j to GraphX App")
spark = SparkSession.builder \
    .appName("Neo4j to GraphX App") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Convert data to GraphFrame
graph_data = fetch_data_from_neo4j(driver)
vertices = spark.createDataFrame(graph_data['nodes'], ["id", "label"])
edges = spark.createDataFrame(graph_data['relationships'], ["src", "dst", "relationship"])
graph = GraphFrame(vertices, edges)

# Analyze the graph using Spark SQL
# Group by model and count downloads
model_downloads = graph.vertices.groupBy("label").count().orderBy(col("count").desc())

# Group by model family and count models
model_families = graph.vertices.groupBy("label").agg(countDistinct("id")).withColumnRenamed("count(DISTINCT id)", "model_count")

# Group by dataset and count models and downloads
dataset_stats = graph.edges.groupBy("relationship").agg(countDistinct("src"), count("relationship")).withColumnRenamed("count(DISTINCT src)", "model_count").withColumnRenamed("count(relationship)", "download_count")

# Print or further process the results
print("Model Downloads Ranking:")
model_downloads.show()

print("Model Families Ranking:")
model_families.show()

print("Dataset Stats:")
dataset_stats.show()

# Optionally, write results back to Neo4j
