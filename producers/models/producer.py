"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient


logger = logging.getLogger(__name__)

BROKER_URL = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    existing_topics = set(t.topic for t in iter(client.list_topics(timeout=5).topics.values()))

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)
        else:
            logger.debug("Topic already exists: %s", self.topic_name)

        schema_registry = CachedSchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
        self.producer = AvroProducer({"bootstrap.servers": BROKER_URL}, schema_registry=schema_registry)

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        logger.info("Creating topic: %s", self.topic_name)
        futures = self.client.create_topics([
            NewTopic(topic=self.topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas),
        ])
        for _, future in futures.items():
            try:
                future.result()
            except Exception as e:
                pass

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
