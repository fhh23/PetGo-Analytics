#python ../Kafka/producer.py  
/usr/local/spark/bin/spark-submit --master spark://ip-172-31-0-173:7077 --py-files /home/ubuntu/Amazon-Go-Recommendation-System-Using-Distributed-Graph-Processing/Spark/common/Itemsets.py SparkBatch.py > out_batch.txt 
