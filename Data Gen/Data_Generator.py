import json
sample = {'time': 12417,  
          'item': 1675, 
          'bought': 1}
d = {"name":"user1",
     "children":[{'name':key,"size":value} for key,value in sample.items()]}
j = json.dumps(d, indent=4)
f = open('../Kafka/sample.json', 'w')
print(j,file=f)
f.close()