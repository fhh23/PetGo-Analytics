import kafka
import json

cluster = kafka.KafkaClient("35.166.234.119:9092")
#producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer = kafka.SimpleProducer(cluster)
jsonFile = open("sample.json", 'r')

for line in jsonFile:
	#producer.send('my-topic',line)
	producer.send_messages('my-topic', line)

