#!/usr/bin/env pyspark

from __future__ import print_function

import os
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pprint

from pyspark import SparkContext, SparkConf
sc = SparkContext(appName='streamingFromKafka')
ssc = StreamingContext(sc, 2)
# Set the Kafka topic
topic = 'fh-topic'

# List the Kafka Brokers
broker_file = open('brokers.txt', 'r')
kafka_brokers = broker_file.read()[:-1]
broker_file.close()
kafkaBrokers = {"metadata.broker.list": "ec2-35-166-31-140.us-west-2.compute.amazonaws.com:9092"}

# Create input stream that pull messages from Kafka Brokers (DStream object)
tweets_raw = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
# Printing kafkastream content 
tweets_raw.pprint()

ssc.start()
ssc.awaitTermination()
