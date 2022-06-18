import traceback
import time
import random
import json
from datetime import datetime
from kafka import KafkaProducer

bootstrap_servers = ['localhost:9092']

topicName = 'stream-heartbeat'

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

movies = ["movie_214", "movie_5", "movie_115", "movie_15"]
c = 1
while(True):
    try:
        message ={
            "userId": "user_987",
            "movieId": movies[c % 4],
            "position": random.randint(1, 5000),
            "timestamp": datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S")
        }
        message = json.dumps(message)
        producer.send(topicName, message.encode('utf-8'))
        time.sleep(0.5)
        print(f"{c} Message Sent")
        print("#" * 80)
        c += 1
    except Exception as e:
        traceback.print_exc()
        break
