"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

BROKER_URL = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro,
        offset_earliest=True,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        self.broker_properties = {
            "bootstrap.servers": BROKER_URL,
            # "compression.type": "lz4",
            'group.id': 'cta-group-05',
            'auto.offset.reset': 'earliest',
            "batch.num.messages": 100,
            # "linger.ms": 1000,
        }

        if is_avro is True:
            logger.info("AVRO consumer for topic: %s", self.topic_name_pattern)
            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            logger.info("Standard consumer for topic: %s", self.topic_name_pattern)
            self.consumer = Consumer(self.broker_properties)

        self.consumer.subscribe(
            [self.topic_name_pattern,],
            on_assign=self.on_assign
        )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING

        logger.debug("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            try:
                num_results = 1
                while num_results > 0:
                    num_results = self._consume()
                await gen.sleep(self.sleep_secs)
            except Exception as e:
                logger.error("ERROR!!! %s", e)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            msg = self.consumer.poll(self.consume_timeout)
        except SerializerError as e:
            logger.error("Message deserialization failed for %s: %s", msg, e)
            return 0

        if msg is None:
            return 0
        if msg.error():
            logger.error("Consumer error: %s", msg.error())
            return 0

        self.message_handler(msg)
        return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
