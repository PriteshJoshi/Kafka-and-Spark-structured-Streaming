from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads


if __name__ == '__main__':
  consumer = KafkaConsumer(
      'numtest',
       bootstrap_servers=['13.212.15.67:9092','13.212.15.67:9093','13.212.15.67:9094'],
       auto_offset_reset='earliest',
       enable_auto_commit=True,
       group_id='my-group',
       value_deserializer=lambda x: loads(x.decode('utf-8')))

  for message in consumer:
      message = message.value
      print(message)
