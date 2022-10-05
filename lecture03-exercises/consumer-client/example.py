from kafka import KafkaConsumer
consumer = KafkaConsumer('virkpls', bootstrap_servers=['kafka:9092'], group_id='group1')

for msg in consumer:
    print (msg)