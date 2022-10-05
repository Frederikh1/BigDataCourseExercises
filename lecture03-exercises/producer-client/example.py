from operator import truediv
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='kafka:9092')
exit = False
while not exit:
    regularInput = input()
    if (regularInput == "stop"):
        exit == True
        break
    serialized = bytes(regularInput, 'utf-8')
    producer.send('virkpls', serialized)
    
#for value in range(100):
 #   message = 'besked' + str(value)
  #  print(message)
   # serialized = bytes(message, 'utf-8')
    #producer.send("virkpls", serialized)
