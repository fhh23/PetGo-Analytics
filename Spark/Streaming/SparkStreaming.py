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

def lineSplit2(lines):
    if (lines):
        word = lines.split(",")
        w1 = word[0]
        w2 = word[4]
        word = (w2, w1)
        #word2 = lines.split(",")[4]
        #tups = zip(word, word2)
        #print ("HI HI \n")
        #print(tups.type)
#        print(word)
        return word
        #return tups
    return "none"


def lineSplit(lines):
    if (lines):
        word = lines.split(",")[0]
        return word
    return "none" 
	
#======================================Apriori algo===================================================
def get_frequent_items_sets(data,min_sup,steps=0):

	# we transform the dataset in a list of sets
	transactions = list()

	# Temporary dictionary to count occurrences
	items = defaultdict(lambda: 0)

	# Returned dictionary
	solution = dict()
	L_set = set()

	# Fills transactions and counts "singletons"
	for line in data:
		transaction = set(line)
		transactions.append(transaction)
		for element in transaction:
			items[element]+=1

	# Add to the solution all frequent items
	for item, count in items.iteritems():
		if count >= min_sup:
			L_set.add(frozenset([item]))

	# Generalize the steps it ends when there are no candidates
	# or if the user provided as input the number of parameters
	k = 2 
	solution[k-1] = L_set
	while L_set != set([]) and k != steps+1:
		L_set = create_candidates(L_set,k)
		C_set = frequent_items(L_set,transactions,min_sup)
		if C_set != set([]): solution[k]=C_set
		L_set = C_set
		k = k + 1
	return solution





# Creates candidates joining the same set
# input:
#    itemSet: a set of frequent items
#     length: the cardinality of generated combinations
#  returns:
#	   set: a set containing all combinations with cardinality equal to length
#		(be carefull on iterating on it)

def create_candidates(itemSet, length):
        return set([i.union(j) for i in itemSet for j in itemSet if len(i.union(j)) == length])




# Checks occurrences of items in transactions and returns frequent items
# input:
#    items: a set of frequent items (candidates)
#     transactions: list of sets representing baskets
#  returns:
#	   _itemSet: a set containing all frequent candidates (a subset of inputs)

def frequent_items(items, transactions, min_sup):
	_itemSet = set()
	counter = defaultdict(int)
	localDict = defaultdict(int)
	for item in items:
		for transaction in transactions:
			if item.issubset(transaction):localDict[item] += 1

	for item, count in localDict.items():
		if count >= min_sup:
			_itemSet.add(item)
	return _itemSet


# helper to standardize the output (not part of the algorithm)

def please_clean(solution):
	res = []
	for i in solution.itervalues():
		for j in i:
			res.append(j)
	return res

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
    print(percentile_limit)

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
#broker_file = open('brokers.txt', 'r')
#kafka_brokers = broker_file.read()[:-1]
#broker_file.close()
kafkaBrokers = {"metadata.broker.list": "ec2-52-33-141-253.us-west-2.compute.amazonaws.com:9092"}

# Create input stream that pull messages from Kafka Brokers (DStream object)
trans = KafkaUtils.createDirectStream(ssc, [topic], kafkaBrokers)
body = trans.map(lambda x: x[1])
lines = body.flatMap(lambda bodys: bodys.split("\r\n")) 
word = lines.map(lineSplit) \
            .map(lambda word: (word, 1)) \
            .reduceByKey(lambda a, b: a+b).cache()
            #.map(lambda x:x[1]) 
#print_word = word.pprint()
word.foreachRDD(lambda rdd: rdd.foreach(redisWr))
print("RDD filtered: \n") 
word.foreachRDD(compute_percentile)
filter_digest = word.transform(filter_most_popular).pprint()#foreachRDD(lambda RDD: print(RDD.collect()))

ssc.start()
ssc.awaitTermination()
