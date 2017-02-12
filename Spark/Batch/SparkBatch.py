from __future__ import print_function

import os
import pyspark
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import pprint
from pyspark import SparkContext, SparkConf
from operator import add
import sys
import boto
from collections import defaultdict
from itertools import imap, combinations
import copy
from string import atoi
import rethinkdb as r

percentile_broadcast = None

def splitFunc(lines):
    if (lines):
        word = lines[0].split(",")
        w1 = word[0]
        w2 = word[4]
        tup = (w2, w1)
        return tup 
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

#======================================================================================================


def rethinkWr(obj):
    conn = r.connect(host='localhost', port=28015, db='test')
    for x in obj:
        count = x[1] 
        objSet = x[0]
        setSize = len(objSet)
        #r.table('itemsets').index_create('length').run(conn)
        for y in objSet:
            if r.table('itemsets').get(y).run(conn) is None:
                r.table('itemsets').insert({
                                "id": y,
                                "length": [setSize],
                                "count" : [count],
                                "set" : [objSet],
                                }).run(conn)
            else:
                r.table('itemsets').get(y).update({'length': r.row['length'].append(setSize)}).run(conn)
                r.table('itemsets').get(y).update({'count': r.row['count'].append(count)}).run(conn)
                r.table('itemsets').get(y).update({'set': r.row['set'].append(objSet)}).run(conn)
    return ""

  
sc = SparkContext(appName='ItemsetsBatch')

# Read in from S3
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'default')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'default')

conn = boto.connect_s3(aws_access_key, aws_secret_access_key)
bucket = conn.get_bucket('fh-data-insight')
#data = sc.testFile("s3n://fh-data-insight/*")
#print(data.take(5)
#conn = r.connect(host='localhost', port=28015, db='test')
#r.table('itemsets').index_create('length').run(conn)
#conn.close()
bucket_path = "s3n://fh-data-insight/"
for f in bucket:
    data = sc.textFile(bucket_path + f.name)
    #print(data.take(5))
    #print("\n")
    #body = data.map(lambda x: x[1])
    #lines = data.flatMap(lambda bodys: bodys.split("\r\n"))
    lines = data.map(lambda line: line.split(' '))
    numPartitions = data.getNumPartitions() 
    s = .3
    #count = lines.count()
    count = 10
    threshold = s*count
    #split string baskets into lists of items
    baskets = lines.map(splitFunc) \
                   .map(lambda (a,b): (int(a), int(b))) \
                   .groupByKey() \
                   .mapValues(list) \
                   .map(lambda x: sorted(x[1]))

    #print(baskets.take(5))
    #treat a basket as a set for fast check if candidate belongs
    basketSets = baskets.map(set).persist()
    #each worker calculates the itemsets of his partition
    localItemSets = baskets.mapPartitions(lambda data: [x for y in get_frequent_items_sets(data, threshold/numPartitions).values() for x in y], True)

    #for reducing by key later
    allItemSets = localItemSets.map(lambda n_itemset: (n_itemset,1))
    #merge candidates that are equal, but generated by different workers
    mergedCandidates = allItemSets.reduceByKey(lambda x,y: x).map(lambda (x,y): x).filter(lambda r: len(r) > 0)
    mergedCandidates = mergedCandidates.collect()
    candidates = sc.broadcast(mergedCandidates)

    #count actual occurrence of candidates in document   
    counts = basketSets.flatMap(lambda line: [(candidate,1) for candidate in candidates.value if line.issuperset(candidate)])
    #filter finalists

    finalItemSets = counts.reduceByKey(lambda v1, v2: v1+v2).filter(lambda (i,v): v>=threshold).filter(lambda (i,v): len(i) > 1)
    #put into nice format
    #finalItemSets = finalItemSets.map(lambda (itemset, count): ", ".join([str(x) for x in itemset])+"\t("+str(count)+")")
    print(finalItemSets.collect())#saveAsTextFile("spark_out.txt")
    finalItemSets.foreachPartition(rethinkWr)
#conn = r.connect(host='localhost', port=28015, db='test')
#r.table('itemsets')['count'].order_by(index=r.desc('length')).run(conn)
#conn.close()
