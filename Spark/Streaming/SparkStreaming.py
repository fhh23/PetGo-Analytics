from __future__ import print_function

import os
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from tdigest import TDigest
import pprint
from pyspark import SparkContext, SparkConf
from operator import add
from collections import defaultdict
from itertools import imap, combinations
import sys
import copy
from string import atoi
import redis

percentile_broadcast = None

def redisWr(obj):
    # define function to put kv pair in redis
    redis_server = 'ec2-52-11-94-102.us-west-2.compute.amazonaws.com'
    r = redis.StrictRedis(host=redis_server, port=6379, db=0)
    r.set(obj[0], obj[1])
    return "none"

def lineSplit(lines):
    if (lines):
        word = lines.split(",")[0]
        return word
    return "none" 

def digestByPartition(values):
    digest = TDigest()
    digest.batch_update(values)
    return [digest] 

def computePercentile(rdd):
    global percentile_broadcast
    percentile_limit = rdd.map(lambda row: int(row[1])) \
                          .mapPartitions(digestByPartition) \
                          .reduce(add) \
                          .percentile(75)
    percentile_broadcast = rdd.context.broadcast(percentile_limit)
    print(percentile_limit)

def filter_most_popular(rdd):
    global percentile_broadcast
    if percentile_broadcast:
        return rdd.filter(lambda row: row[1] > percentile_broadcast.value)
    return rdd.context.parallelize([])

# Define context and streaming window
sc = SparkContext(appName='streamingTDigest')
ssc = StreamingContext(sc, 15)

# Set the Kafka topic
topic = 'fh-topic'

# List the Kafka Brokers
kafkaBrokers = {"metadata.broker.list": "ec2-52-33-141-253.us-west-2.compute.amazonaws.com:9092"}

# Create input stream that pull messages from Kafka Brokers as DStream object
trans = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)

# Convert an entire body of transactions (1 line is one transaction for one customer) 
# to a list of items and their count
itemCounts = trans.map(lambda x: x[1]) \
                  .flatMap(lambda bodys: bodys.split("\r\n")) 
                  .map(lineSplit) \
                  .map(lambda word: (word, 1)) \
                  .reduceByKey(lambda a, b: a+b).cache()

# Write (item, count) key-value pair to Redis
itemCounts.foreachRDD(lambda rdd: rdd.foreach(redisWr))

# Compute count value representing percentile needed
itemCounts.foreachRDD(computePercentile)


filter_digest = itemCounts.transform(filter_most_popular).pprint()

ssc.start()
ssc.awaitTermination()
