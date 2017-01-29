import kafka
import json
import boto3
import time

#s3_client.download_file('MyBucket', 'hello-remote.txt', 'hello2.txt')
def get_s3_bucket(bucket_name):
	s3 = boto3.resource('s3')
	try:
		s3.meta.client.head_bucket(Bucket=bucket_name)
	except botocore.exceptions.ClientError as e:
		# If a client error is thrown, check that it was a 404 error.
		# If it was a 404 error, then the bucket does not exist.
		error_code = int(e.response['Error']['Code'])
		if error_code == 404:
			exists = False
			print (e.response['404 Error: bucket not found'])
		else:
			print (e.response['Error'])
		return None
	else:
		return s3.Bucket(bucket_name)

#cluster = kafka.KafkaClient("35.166.31.140:9092")
#producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')
#producer = kafka.SimpleProducer(cluster)

producer = kafka.KafkaProducer(bootstrap_servers='35.166.31.140:9092')

bucket_name = 'fh-data-insight'
bucket = get_s3_bucket(bucket_name)
for obj in bucket.objects.limit(1):
	obj_body = obj.get()['Body']
	json_body = obj_body.read()
	producer.send('fh-topic2', json_body)
	time.sleep(5)
	print json_body


# #jsonFile = open("sample.json", 'r')
# textFile = open('hello2.txt')

# for line in jsonFile:
	# #producer.send('my-topic',line)
	# producer.send_messages('my-topic', line)

