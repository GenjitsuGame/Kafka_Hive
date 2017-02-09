#! /usr/bin/env python
# -*- coding: utf-8 -*-

import time

from pykafka.common import OffsetType
from kafka import KafkaConsumer
from kafka import TopicPartition
import pyhs2

topic = TopicPartition('my-topic',0)

def get_consumer_kafkaConsumer():
        consumer = KafkaConsumer(group_id='my-group1',bootstrap_servers=['ip-172-31-15-110.us-west-2.compute.internal:6667','ip-172-31-15-237.us-west-2.compute.internal:6667','ip-172-31-5-184.us-west-2.compute.internal:6667'])
        consumer.assign([topic])
        position = consumer.position(topic)
        consumer.seek_to_end(topic)
        return consumer

def get_tweet(consumer):
    try:
        for message in consumer:
            if message is not None:
                #print message.offset, message.value, time.strftime("%Y%m")
	 	with pyhs2.connect(host='localhost',
                	           port=10000,
                        	   authMechanism="PLAIN",
                	           user='hive',
                	           password='admin',
                	           database='default') as conn:
            		with conn.cursor() as cur:
                #Show databases
                		print cur.getDatabases()

                #Execute query
                		cur.execute("create table if not exists twitter_grippe(ID varchar(255), tweet string, date_month string)")
                		cur.execute("INSERT INTO table twitter_grippe values ('{}','{}','{}')".format(message.offset, message.value, time.strftime("%Y%m")))
            elif not message:
                print 'No message'
            else:
                print 'Something else happened..'
    except KeyboardInterrupt as e:
            pass
    return message.offset, message.value, time.strftime("%Y%m")



if __name__ == '__main__':
        consumer = get_consumer_kafkaConsumer()
        id, message, date_month = get_tweet(consumer)
        print id, message, date_month
      
