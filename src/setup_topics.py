import os

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "telco.activity.raw"

admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
try:
    admin.create_topics([NewTopic(name=TOPIC, num_partitions=3, replication_factor=1)])
    print(f"Created topic {TOPIC}")
except TopicAlreadyExistsError:
    print(f"Topic {TOPIC} already exists")
finally:
    admin.close()
