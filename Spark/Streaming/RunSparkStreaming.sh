#python ../Kafka/producer.py  
/usr/local/spark/bin/spark-submit --master spark://ip-172-31-0-173:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 SparkStreaming.py > sample_out.txt 
