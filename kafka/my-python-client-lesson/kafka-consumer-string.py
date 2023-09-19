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
        # Here we are using raw SQL, You can also use ORMs like SQLAlchemy.
        # For cockroachdb datatypes, see https://www.cockroachlabs.com/docs/stable/data-types
        # Readoff the data from event
        #Kafka seems to use 13digit epoch time, so we need to convert to equivalent of 10 digit by dividing by 1000
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
        # Define regular expressions for each prefix
        # info_pattern = re.compile(r'^INFO\s', re.IGNORECASE)
        # warn_pattern = re.compile(r'^WARN\s', re.IGNORECASE)
        # error_pattern = re.compile(r'^ERROR\s', re.IGNORECASE)
        message = formatedValue
        
        # if info_pattern.match(eventValue):
        #         prefix = "INFO"
        #         message = re.search(r'INFO (.*)', eventValue)
        #         print(message)
        # elif warn_pattern.match(eventValue):
        #         prefix = "WARN"
        #         message = re.search(r'WARN (.*)', eventValue)
        #         print(message)
        # elif error_pattern.match(eventValue):
        #     prefix = "ERROR"
        #     message = re.search(r'ERROR (.*)', eventValue)
        #     print(message)
        # else:
        #     prefix = "UNKNOWN"  # Default case if no prefix matches
        #     message = eventValue
        #     print(message)
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