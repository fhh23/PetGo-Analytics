from collections import defaultdict
from itertools import imap, combinations
import sys
import copy
from string import atoi
from pyspark import SparkContext, SparkConf
import apriori

#======================================Apriori algo===================================================
def get_frequent_items_sets(transactions,min_support,steps=0):
	frequent_itemsets = []
	items = defaultdict(lambda: 0)
	[inc(items,item,1) for transaction in transactions for item in transaction]
	items = set(item for item, support in items.iteritems()
		if support >= min_support)
	[frequent_itemsets.append(item) for item in items]
	transactions = [set(filter(lambda v: v in items, y)) for y in transactions]
	count = 2
	while bool(len(items)) and count != steps:
		candidates = combinations([i for i in items],count)
		items = defaultdict(lambda: 0)
		[inc(items,candidate,1) for candidate in candidates for transaction in transactions if transaction.issuperset(candidate)]
		[frequent_itemsets.append(item) for item,support in items.iteritems() if support >= min_support]
		items = set(element for tupl in items.iterkeys() for element in tupl)
		count+=1
	return frequent_itemsets

def inc(dic,key,val):
	dic[key]+=val
#======================================================================================================

def findFrequentItemsets(data):
    numPartitions = 1
    s = .3
    count = data.count()
    threshold = s*count
    #split string baskets into lists of items
    baskets = data.map(lambda line: sorted([int(y) for y in line.strip().split(' ')]))
    #treat a basket as a set for fast check if candidate belongs
    basketSets = baskets.map(set).persist()
    #each worker calculates the itemsets of his partition
    localItemSets = baskets.mapPartitions(lambda data: [x for y in apriori.get_frequent_items_sets(data, threshold/numPartitions).values() for x in y], True)
    #for reducing by key later
    allItemSets = localItemSets.map(lambda n_itemset: (n_itemset,1))
    #merge candidates that are equal, but generated by different workers
    mergedCandidates = allItemSets.reduceByKey(lambda x,y: x).map(lambda (x,y): x)
    #distribute global candidates to all workers
    mergedCandidates = mergedCandidates.collect()
    candidates = sc.broadcast(mergedCandidates)
    #count actual occurrence of candidates in document
    counts = basketSets.flatMap(lambda line: [(candidate,1) for candidate in candidates.value if line.issuperset(candidate)])
    #filter finalists
    finalItemSets = counts.reduceByKey(lambda v1, v2: v1+v2).filter(lambda (i,v): v>=threshold)
    #put into nice format
    finalItemSets = finalItemSets.map(lambda (itemset, count): ", ".join([str(x) for x in itemset])+"\t("+str(count)+")")
    finalItemSets.foreachRDD(lambda RDD: print(RDD.collect()))
    return finalItemSets