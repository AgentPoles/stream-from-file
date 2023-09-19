from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('connect-file-pulse-raw-logs-02', bootstrap_servers=['broker:9092'], auto_offset_reset='earliest')
for msg in consumer:
    # write to database.
    print(msg.value)