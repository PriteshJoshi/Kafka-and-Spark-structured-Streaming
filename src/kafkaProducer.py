from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

from kafka.errors import KafkaError

if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['18.142.146.207:9092'],
                             acks=1, # 0, 1, all
                             value_serializer=lambda x:dumps(x).encode('utf-8'))

    items = ['a', 'b', 'c', 'd', 'e','f','g','h','i','j','k']
    low_limit = 5
    high_limit = 10
    for e in range(1000):

        data = {'item': random.choice(items),
                'quantity': random.randint( low_limit, high_limit),
               }

        future = producer.send('orders', value=data)

        try:
          record_metadata = future.get(timeout=10)

        except KafkaError:
          print(KafkaError)

        print(str(e)+ " data sent to topic " + record_metadata.topic +" Partition "+ str(record_metadata.partition) +
              " offset " +str(record_metadata.offset))

        sleep(1)

    producer.close()

