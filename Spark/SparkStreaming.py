from __future__ import print_function

import os
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from tdigest import TDigest
import pprint
from pyspark import SparkContext, SparkConf
from operator import add
import Itemsets

percentile_broadcast = None

def digest_partitions(values):
    digest = TDigest()
    digest.batch_update(values)
    return [digest] 

def compute_percentile(rdd):
    global percentile_broadcast
    percentile_limit = rdd.map(lambda row: int(row[1])) \
                          .mapPartitions(digest_partitions) \
                          .reduce(add) \
                          .percentile(50)
    percentile_broadcast = rdd.context.broadcast(percentile_limit)

def filter_most_popular(rdd):
    global percentile_broadcast
    if percentile_broadcast:
        return rdd.filter(lambda row: row[1] > percentile_broadcast.value)
    return rdd.context.parallelize([])

sc = SparkContext(appName='streamingFromKafka')
ssc = StreamingContext(sc, 15)
# Set the Kafka topic
topic = 'fh-topic'

# List the Kafka Brokers
broker_file = open('brokers.txt', 'r')
kafka_brokers = broker_file.read()[:-1]
broker_file.close()
kafkaBrokers = {"metadata.broker.list": "ec2-35-166-31-140.us-west-2.compute.amazonaws.com:9092"}

# Create input stream that pull messages from Kafka Brokers (DStream object)
trans = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
body = trans.map(lambda x: x[1])#.foreachRDD(lambda RDD: print(RDD.collect()))
lines = body.flatMap(lambda bodys: bodys.split("\r\n"))#.foreachRDD(lambda RDD: print(RDD.collect())) 
word = lines.map(Itemsets.lineSplit) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a+b)
            #.map(lambda x:x[1]) 
print_word = word.pprint()
print("RDD filtered: \n") 
word.foreachRDD(compute_percentile)
filter_digest = word.transform(filter_most_popular).foreachRDD(lambda RDD: print(RDD.collect()))
Itemsets.findFrequentItemsets(lines)

#f = open('digest.txt', 'a')
#print(" OKOKOK ", file=f)
#print(digest.percentile(50), file=f)
#print("\n", file=f)
#f.close()

#filter_word = word.filter(lambda x: x <= digest.percentile(50)).foreachRDD(lambda RDD: print(RDD.collect()))
# Printing kafkastream content 
#counts.pprint()

ssc.start()
ssc.awaitTermination()
