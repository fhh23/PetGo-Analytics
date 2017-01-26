import json
import boto3

s3_client = boto3.client('s3')
open('hello.txt', 'w').write("To be, or not to be,--that is the question:--" + 
      "Whether 'tis nobler in the mind to suffer" + "The slings and arrows of outrageous fortune" +
      "Or to take arms against a sea of troubles")

# Upload the file to S3
s3_client.upload_file('hello.txt', 'fh-data-insight', 'hello-remote.txt')
		
# sample = {'time': 12417,  
          # 'item': 1675, 
          # 'bought': 1}
# d = {"name":"user1",
     # "children":[{'name':key,"size":value} for key,value in sample.items()]}
# j = json.dumps(d, indent=4)
# f = open('../Kafka/sample.json', 'w')
# print(j,file=f)
# f.close()