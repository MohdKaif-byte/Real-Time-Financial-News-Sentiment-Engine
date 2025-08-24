import json
import os
import time
import logging
from kafka import KafkaConsumer
import pandas as pd
from filelock import FileLock
import signal
from typing import Set, List, Dict 
import datetime

# ---- LOGGING SETUP ----
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
) # its just a way to print useful messages like when the program starts and when it connects and if something goes wrong
logger = logging.getLogger(__name__) #the format will show Date and time, the type of message(Info, Error etc) and the message itself

def shutdown_handler(signum, frame):
    logger.info("Received shutdown signal") #this function is called when the app is told to stop
    raise KeyboardInterrupt # Helps in exiting from the code or pipeline

signal.signal(signal.SIGTERM, shutdown_handler) # it tells python to run the shutdown handler function when it gets a stop signal from the system
# so program doesn't crash just shuts doen in a controlled
# ---- KAFKA CONSUMER SETUP ----
def create_consumer():
    consumer = KafkaConsumer(
        'financial-news-sentiment', # This is the topic of kafka from where we wanna read information or news here
        bootstrap_servers='kafka:9092', # This is the kafka server we are connecting to 
        group_id='news-consumer-group', # to keep track of message what has been read and where to stop and from where to resume
        auto_offset_reset='earliest', # start from beginning if no offset is found
        enable_auto_commit=True, # kafka would automatically mark messages as read
        session_timeout_ms = 30000, # if no heart beat for 30 sec kafka think consumer is gone
        heartbeat_interval_ms = 10000, # send a signal every 10 sec to tell "i am alive"
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) # convert kafka message which is in bytes to python dictionary
    )
    logger.info("Kafka consumer created and subscribed")
    return consumer

# ---- CSV HANDLING ----
CSV_PATH = '/app/shared_data/sentiment_data.csv' # the path where the data would be saved

def initialize_csv():
    if not os.path.exists(CSV_PATH):
        with FileLock(CSV_PATH + ".lock"): # file lock is just used that if one time a file is getting used other operation do not mess up with or write something
            pd.DataFrame(columns=[ # This is the columns with which the files have been initialized
                'title', 'source', 'publishedAt', 'sentiment', 'confidence', 'link'
            ]).to_csv(CSV_PATH, index=False)
        logger.info("CSV file initialized") # at the same tiime consumer writes it and dashboard reads it so no interference

initialize_csv()

# ---- WRITE TO CSV ----
def write_to_csv(records: List[Dict]): # here the records are the list of messages which are dictionaries
    try: # Tells python these are the columns u should expect
        expected_columns = ['title', 'source', 'publishedAt', 'sentiment', 'confidence', 'link']
        df = pd.DataFrame(records) # columns were specified here because earlier files were getting 
        for col in expected_columns: # saved in the wrong column basically there were column mismatch
            if col not in df.columns: # if any expected column is missing. it creates an empty one so the file won'tt break
                df[col] = ""
        df = df[expected_columns] # reorders column to match the order present in the file

        with FileLock(CSV_PATH + ".lock", timeout=10): # locks the file so only one program can write at a time and wait for 10 sec if its busy or someone else is writing  do not throw the error right away
            write_header = False # since the file already exists with columns here the header is false
            if not os.path.exists(CSV_PATH): #if the file doesn't exist add column headers to the first row
                write_header = True
            else:
                existing_cols = pd.read_csv(CSV_PATH, nrows=0).columns.tolist()
                if existing_cols != expected_columns:
                    write_header = True # if the file exists but the column doesn't match write it with correct header

            if write_header:
                df.to_csv(CSV_PATH, mode='w', header=True, index=False, encoding='utf-8')
                logger.info(f"CSV file created/reset with headers: {expected_columns}") # if writing th efile for the first tiime or fixing header write from scratch with headers
            else:
                df.to_csv(CSV_PATH, mode='a', header=False, index=False, encoding='utf-8') # if everything is fine append the row at the last
            logger.info(f"Wrote {len(records)} records") # log how many records are written
    except Exception as e:
        logger.error(f"CSV write failed: {str(e)}")


# ---- MAIN LOOP ----
def process_messages(consumer): # This is the main loop that keeps checking new messages and writes them to the csv file
    seen = set() # This is used to avoid duplicates
    buffer = [] # buffer temporarily stores messages before writing them in a batch

    while True: #keeps running forever until the program is stopped
        records = consumer.poll(timeout_ms=1000) # ask kafka if there are any other messages wait for 1 seconds
        if not records:
            time.sleep(1) # if not wait for 1 sec and then try again
            continue

        for tp, messages in records.items():
            for message in messages: # go through each message that kafka has sent
                article = message.value # this is the actual news data
                logger.info(f"Received message keys: {list(article.keys())}")
                logger.info(f"Received message values: {article}")


                # The code is to ignore if there is any missing key
                required_keys = ['title', 'source', 'publishedAt', 'sentiment', 'confidence', 'link'] # Thses are the information that have been required
                if not all(k in article for k in required_keys):
                    logger.warning("Skipping invalid message format")
                    continue # it gives the warning and skips the message if any of the needed data is missing

                unique_key = (article['title'], article['publishedAt'])
                if unique_key in seen:
                    logger.info(f"Duplicate message skipped: {unique_key}") # Skipping duplicate message as already have been taken care of
                    continue # skip the message if the title and publishedAt has already seen before

                seen.add(unique_key) # add this data to seen ones
                if len(seen) > 10000:
                    seen.clear() # just to prevent lot  of data all at once
                    logger.info("Cleared seen set to prevent memory bloat")

                buffer.append({ # appends all the data into the buffer for now
                    'title': article['title'],
                    'source': article['source'],
                    'publishedAt': article['publishedAt'],
                    'sentiment': article['sentiment'],
                    'confidence': round(float(article['confidence']), 3),
                    'link': article['link']
                })

                if len(buffer) >= 10: # once bufer reaches length of 10 write down the data to csv and clear buffer
                    write_to_csv(buffer)
                    buffer = []

        if buffer: # if we have leftover messages in buffer write them anyway
            write_to_csv(buffer)
            buffer = [] # empyting for the next batch

if __name__ == "__main__": # start here when the  file is run
    try:
        consumer = create_consumer()
        process_messages(consumer) # connects to kafka and starts checking for new messages forever
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
    finally: # if the program gets interrupted close it safely
        if 'consumer' in locals():
            consumer.close()



