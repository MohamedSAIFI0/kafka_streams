"""
setup_topics.py  –  Création des topics Kafka (Étape 1)
Big Data 2026 - Mr. Abdelmajid BOUSSELHAM

Prérequis : pip install confluent-kafka
"""

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"

TOPICS = [
    "text-input",
    "text-clean",
    "text-dead-letter",
]

def create_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    new_topics = [
        NewTopic(name, num_partitions=1, replication_factor=1)
        for name in TOPICS
    ]

    results = admin.create_topics(new_topics)

    for topic, future in results.items():
        try:
            future.result()
            print(f"✅ Topic créé     : {topic}")
        except Exception as e:
            print(f"⚠️  Topic ignoré   : {topic}  ({e})")

if __name__ == "__main__":
    create_topics()
