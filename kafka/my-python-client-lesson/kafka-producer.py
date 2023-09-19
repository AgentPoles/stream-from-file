from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('quickstart-events', b'message- from-python')
for n in range(3):
    message = 'additional message {} from python'.format(n)
    producer.send('quickstart-events',bytearray(message, 'utf-8'))
    
producer.flush()