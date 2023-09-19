# Importing our dependencies
from kafka import KafkaConsumer
from cockroach_connect import getConnection
import logging
from datetime import datetime
import re

# Get mandatory connection
conn = getConnection(True)
# Define how to write to database
def cockroachWrite(event):
    try:
        conn.autocommit = True
        timestamp = int(event.timestamp/1000)
        eventTimestamp = datetime.fromtimestamp(timestamp) #convert event.timestamp from epoch to datetime
        eventValue = (event.value).decode('utf-8', errors='ignore').replace('\x00', '')
        formatedValue = ""
        individual_events = re.split(r', \[', ''.join(eventValue))
     

        for event in individual_events:
            # Find the index of the closing square bracket ']' after the timestamp
            closing_bracket_index = event.find(']')
            
            if closing_bracket_index != -1:
                # Extract the value portion of the log event
                formatedValue = event[closing_bracket_index + 2:]  # +2 to skip the ']' and space
                print(formatedValue)
            else:
                print("Invalid log event format.")
       
        message = formatedValue

        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS FileLogs (id SERIAL PRIMARY KEY, timestamp TIMESTAMP, INFO_or_WARN_or_ERROR STRING)")
            logging.debug("create_accounts(): status message: %s",cur.statusmessage)
            if eventTimestamp and eventValue:
                cur.execute("INSERT INTO FileLogs (timestamp, INFO_or_WARN_or_ERROR) VALUES (%s, %s)", (eventTimestamp, message))
                conn.commit()
    except Exception as e:
        logging.error("Problem writing to database: {}".format(e))

consumer = KafkaConsumer('connect-file-pulse-raw-logs-02', bootstrap_servers=['broker:9092'], auto_offset_reset='earliest')
for msg in consumer:
    # write to database.
    cockroachWrite(msg)