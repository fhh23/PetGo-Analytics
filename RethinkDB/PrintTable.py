from __future__ import print_function
import rethinkdb as r

conn = r.connect(host='localhost', port=28015, db='test')
cursor = r.table("itemsets").run(conn)
for document in cursor:
    print(document)
