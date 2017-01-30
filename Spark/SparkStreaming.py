import os
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pprint

# Set the Kafka topic
topic = "twitter_stream_new"

# List the Kafka Brokers
broker_file = open('kafka_brokers.txt', 'r')
kafka_brokers = broker_file.read()[:-1]
broker_file.close()
kafkaBrokers = {"metadata.broker.list": kafka_brokers}

# Create input stream that pull messages from Kafka Brokers (DStream object)
tweets_raw = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)

# Printing kafkastream content 
kafka_stream.pprint()