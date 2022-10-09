from math import prod
from hdfs import InsecureClient
from kafka import KafkaProducer

# Create an insecure client that can read from HDFS
client = InsecureClient('http://namenode:9870', user='root')

# Read the alice in wonderland text file from HDFS


# Reading a file, using a delimiter makes it return a list
with client.read('/alice-in-wonderland.txt', encoding='utf-8', delimiter='\n') as reader:
# Write each sentence in alice in wonderland to a kafka topic with a KafkaProducer
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    for line in reader:
        serialized = bytes(line, 'utf-8')
        producer.send('alice2', serialized)
    producer.send('alice2', bytes('FINISHED', 'utf-8'))
    producer.flush()

