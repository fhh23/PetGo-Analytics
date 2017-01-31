from tdigest import TDigest
from numpy.random import random

digest = TDigest()
for x in range(5000):
    digest.update(random())

print(digest.percentile(15))
