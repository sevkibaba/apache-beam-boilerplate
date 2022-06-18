from kafka import KafkaConsumer

import sys

bootstrap_servers = ['localhost:9092']

topicName = 'stream-heartbeat'

consumer = KafkaConsumer(topicName, group_id='group1', bootstrap_servers=bootstrap_servers)

for msg in consumer:
    print("Topic Name=%s,Message=%s"%(msg.topic,msg.value))

print("Terminate the script")
sys.exit()
