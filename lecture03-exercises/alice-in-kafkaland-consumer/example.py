from hdfs import InsecureClient
from kafka import KafkaConsumer


# Create a KafkaConsumer that consumes messages from the topic 'alice-in-kafkaland'
consumer = KafkaConsumer('alice2', bootstrap_servers=['kafka:9092'], group_id='group1',enable_auto_commit=True)
consumer.poll()
consumer.seek_to_end()

# Combine all the messages into a single string
fileContents = ''

for msg in consumer:
    eventValue = msg.value.decode('utf-8')
    if(eventValue == 'FINISHED'):
        break
    fileContents += eventValue
    fileContents += "\n"
consumer.close()
print(fileContents)
    
    
# Write the string to HDFS in a file called 'alice-in-kafkaland.txt'
client = InsecureClient('http://namenode:9870', user='root')
with client.write('/alice-in-kafkaland2.txt', encoding='utf-8', overwrite=True) as writer:
    writer.write(fileContents)
    writer.flush()
  
# Reading a file, using a delimiter makes it return a list

