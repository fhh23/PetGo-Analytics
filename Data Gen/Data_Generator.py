import json
import boto3

s3_client = boto3.client('s3')


# Upload the file to S3
s3_client.upload_file('transactions_fixed.txt', 'fh-data-insight', 'transactions_fixed.txt')
		
# sample = {'time': 12417,  
          # 'item': 1675, 
          # 'bought': 1}
# d = {"name":"user1",
     # "children":[{'name':key,"size":value} for key,value in sample.items()]}
# j = json.dumps(d, indent=4)
# f = open('../Kafka/sample.json', 'w')
# print(j,file=f)
# f.close()