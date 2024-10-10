from kafka import KafkaConsumer
from neo4j import GraphDatabase
import json

# Connect to Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "neo4j123"
driver = GraphDatabase.driver(uri, auth=(username, password))

def save_to_neo4j(data):
    with driver.session() as session:
        # Extract relevant fields from JSON
        model_id = data.get("modelId", "")
        sha = data.get("sha", "")
        last_modified = data.get("lastModified", "")
        author = data.get("author", "")
        likes = data.get("likes", 0)
        downloads = data.get("downloads", 0)

        # Cypher query to create or update nodes and relationships
        query = """
        MERGE (m:Model {id: $model_id, sha: $sha, last_modified: $last_modified})
        SET m.author = $author, m.likes = $likes, m.downloads = $downloads
        """

        # Run the query
        session.run(query, model_id=model_id, sha=sha, last_modified=last_modified,
                    author=author, likes=likes, downloads=downloads)

# Connect to Kafka
consumer = KafkaConsumer(
    'quickstart',
    bootstrap_servers=['dip-3:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Consume messages from Kafka
for message in consumer:
    if message.topic == 'quickstart':
        print(message.value)
        print("-----------------------------------------------------")
        json_data = message.value
        save_to_neo4j(json_data)
        

