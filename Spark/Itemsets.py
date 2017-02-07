from __future__ import print_function
from collections import defaultdict
from itertools import imap, combinations
import sys
import copy
from string import atoi
from pyspark import SparkContext, SparkConf
import apriori

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
	k = 0
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

def findFrequentItemsets(data):
    numPartitions = 1
    s = .3
    #count = data.count().toInt()
    count = 1000
    threshold = s*count
    #split string baskets into lists of items
    baskets = data.map(lambda line: sorted([int(y) for y in Itemsets.lineSplit2]))
    #treat a basket as a set for fast check if candidate belongs
    #basketSets = baskets.map(set).persist()
    #each worker calculates the itemsets of his partition
    localItemSets = baskets.mapPartitions(lambda data: [x for y in get_frequent_items_sets(data, threshold/numPartitions).values() for x in y], True)
    #for reducing by key later
    allItemSets = localItemSets.map(lambda n_itemset: (n_itemset,1))
    #merge candidates that are equal, but generated by different workers
    mergedCandidates = allItemSets.reduceByKey(lambda x,y: x).map(lambda (x,y): x)
    #distribute global candidates to all workers
    mergedCandidates = mergedCandidates.foreachRDD(lambda RDD: RDD.collect())
    candidates = sc.broadcast(mergedCandidates)
    #count actual occurrence of candidates in document
    counts = baskets.map(set).flatMap(lambda line: [(candidate,1) for candidate in candidates.value if line.issuperset(candidate)])    
#counts = basketSets.flatMap(lambda line: [(candidate,1) for candidate in candidates.value if line.issuperset(candidate)])
    #filter finalists
    finalItemSets = counts.reduceByKey(lambda v1, v2: v1+v2).filter(lambda (i,v): v>=threshold)
    #put into nice format
    finalItemSets = finalItemSets.map(lambda (itemset, count): ", ".join([str(x) for x in itemset])+"\t("+str(count)+")")
    finalItemSets.foreachRDD(lambda RDD: print(RDD.collect()))
    return finalItemSets
