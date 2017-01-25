import os
import sys
from confluent_kafka import Producer
from random import random

p = Producer({'bootstrap.servers': '{}'.format(os.environ['KAFKA_BROKERS'])})



def delivery_callback (err, msg):
    if err:
        sys.stderr.write('%% Message failed delivery: %s\n' % err)
    else:
        #sys.stderr.write('%% Message delivered to %s [%d]\n' % \
        #                 (msg.topic(), msg.partition()))
        pass

# Produce each line to Kafka
while True:
    msg = "{}, some message for kafka".format(int(random()*1000000))
    try:
        # Produce line (without newline)
        p.produce("messages", msg.encode('utf-8'), callback=delivery_callback)
        
    except BufferError as e:
        sys.stderr.write('%% Local producer queue is full ' \
                         '(%d messages awaiting delivery): try again\n' %
                         len(p))
        p.flush()

    # Serve delivery callback queue.
    # NOTE: Since produce() is an asynchronous API this poll() call
    #       will most likely not serve the delivery callback for the
    #       last produce()d message.
    p.poll(0)