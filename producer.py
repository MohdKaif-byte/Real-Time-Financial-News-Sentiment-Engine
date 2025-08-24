# ---- IMPORTS -----
import requests
from datetime import datetime
import time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import sys
import os

# In Entire code individual functions have been used and try and except approach
# is used for clean code and proper Error Handling

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092") # This is the address where the producer willl send the data

# ---- CONFIGURATION ------

API_KEY = "8b81ecffcd6241a0b58d1bd92bed7ac2" # This is the api key used to fetch the news
COUNTRIES = ['us', 'cn', 'jp', 'kr', 'gb', 'de', 'fr', 'it', 'es', 'nl', 'ch'] # These are the country codes from where the financial news will be fetched

# ---- KAFKA PRODUCER INITIALIZATION ----
# Here the Kafka Topic has been created via Admin API

TOPIC_NAME = 'financial-news' # This is the name of the kafka topic fetched news will be send

def create_kafka_producer(): # this function does two things 1) creates kafka topi if doesn't exist and setup the producer to send message
    admin = None
    retries = 5
    delay = 5

    for attempt in range(1, retries + 1): # try to setup up to 5 times
        try:
            admin = KafkaAdminClient(  # Kafka topic creation, connects to kafka tomanage topic like creation or checking
                bootstrap_servers=KAFKA_BROKER, # This tells where exactly kafka is
                api_version_auto_timeout_ms=30000 # Wait up to 30 seconds to figure out Kafka’s version. Helps avoid timeout errors.
            )

            topic = NewTopic( # defines a news kafka topic named 'financial-news'
                name=TOPIC_NAME,
                num_partitions=1, # the topic is kept as a whole and not divided into parts to keep some portion of the message for parallel processing
                replication_factor=1, # only one copy is kept because the setup is simple but repliccations are done so if something breaks data doesn't get lost
                topic_configs={
                    "retention.ms": "604800000", # Keep message for 7 days,
                    "cleanup.policy": "delete" # Delete the old message
                }
            )

            existing_topics = admin.list_topics() # gives the list of all the topics that exist
            if TOPIC_NAME not in existing_topics:
                admin.create_topics([topic]) # if the 'financial-news' topic hasn't been created now create it
                print(f"Topic '{TOPIC_NAME}' has been created")
            break  # exit loop if successful

        except Exception as e: # if something fails print the error and retry.
            print(f"Failed to setup Kafka topic (attempt {attempt}/{retries}): {e}")
            if attempt == retries:
                sys.exit(1)
            time.sleep(delay) # wait for 5 secs before retrying 

        finally:
            if admin:
                admin.close() # close the admin after useso when we know that admin ha sbeen created disconnect from kafka topic and free up space

    # Producer Setup
    try:
        producer = KafkaProducer(  # This will create actual producer that will send the message
            bootstrap_servers=KAFKA_BROKER, # This tells where the kafka is 
            value_serializer=lambda v: json.dumps(v).encode('utf-8'), # this tells convert the message to JSON and then to bytes efore sending it
            api_version_auto_timeout_ms=30000, # This is the wait time for kafka version check
            request_timeout_ms=30000, #Wait for 30 secs to get a response from kafka 
            retries=5 # Try 5 times if sending fails
        )

        # Testing connection
        producer.send(TOPIC_NAME, {"message": "Kafka connection test"}) #Testing connection so as to do ensure proper flow in the pipeline, sends a test message to cherck if the kafka connection works
        producer.flush() # makes sures theta the message has actually been sent and is not sitting in th memory
        print("Connected to Kafka broker")
        return producer # return the producer object so it can be used later to send real news 

    except Exception as e: # if the connection fails, print the error and stop the program
        print(f"Failed to connect to Kafka: {e}")
        sys.exit(1)

producer = create_kafka_producer()

def safe_send_with_retry(producer, topic, value, max_retries=3): # This function tries to send a message, and if it fails, it tries again
    for attempt in range(max_retries): # Trying 3 times if there is some issues in creating kafka producer
        try:
            future = producer.send(topic, value=value) # send message to given kafka topic
            future.get(timeout=10) # wait maximum 10 seconds for confirmation from kafka 
            return True
        except Exception as e: # if it fails try 3 times
            if attempt == max_retries - 1:
                print(f"Final send failure after {max_retries} attempts: {e}")
                return False
            time.sleep(2 ** attempt)  # Exponential backoff so if first try wait 1 sec, 2nd try wait for 2 sec and for third wait for 4 sec
# ---- NEWS FETCHING & PROCESSING ----

def fetch_articles():
    all_articles = [] # This creates an empty list to collect all the news 
    for country in COUNTRIES: # goes through each country
        try: # The following API key has been used to fetch the news from the newsapi and all of the news fetched are financial news
            url = f"https://newsapi.org/v2/top-headlines?category=business&language=en&country={country}&pageSize=30&apiKey={API_KEY}" # fetches upto 30 financial news
            response = requests.get(url, timeout=10) # sends a get request to URL and wait for 10 secs
            if response.status_code == 429: # if the api limit has reached it gives error code 429
                print("Daily Limit Reached") # This tells that the limit has already been reached of fetchinng the news from the API
                return None # stop everything and return None as nothing will be fetched
            if not response.ok: # if there is any issue with any specific country print the error and continue
                print(f"API Error ({response.status_code}) for {country}")
                continue    
            data = response.json() # convert the response which is JSON format into python dictionary
            if data.get("status") == "ok": # makes sure if the response from the api is successful
                all_articles.extend(data.get("articles", [])) # add the list of articles to all_articles list, if there is nothing just add empty list
                
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Error With {country}: {str(e)}") # shows error for specific country
    
    return all_articles # after going through all the countries return the list of all the news aricles collected

def process_article(article):
    try:
        iso_time = article['publishedAt'] # The following steps are used to convert the time to proper
        dt = datetime.strptime(iso_time, "%Y-%m-%dT%H:%M:%SZ")# Datetime format for better readability
        title = article.get('title')
        link = article.get('url', '')
        if isinstance(title, bytes): # if the title is in bytes decode it to regular text
            title = title.decode('utf-8', errors='ignore')
        source_name = article['source'].get('name', 'Unknown Source')
        if isinstance(source_name, bytes): # if the source names are in byteconvert it to the text
            source_name = source_name.decode('utf-8', errors='ignore')
        return { # Following are the set of informations that are finally being taken from the API
            'title': title,
            'source': source_name,
            'publishedAt': dt.strftime("%Y-%m-%d %H:%M:%S"),
            'link' : link,
            'fetched_at' : time.time() # This will later be used in consumer to apply watermark requirement of the project
        }
    except (KeyError, ValueError) as e: # if there is something or missing in the article catch it
        print(f"Invalid article: {str(e)}")
        return None


# ---- MESSAGE SENDING ----

def send_message(message): # it sends1 message to kafk aif it fails it tries again 3 times
    for attempt in range(3):
        try:
            print(f"Sending message: {message}") # just prints the message being sent
            print(f"Message type: {type(message)}") # prints what type of object the message is useful in debugging
            # The code below sends the messsage to kafka topic for further operations which will be done by the spark
            future = producer.send(TOPIC_NAME, value=message, # this is the main line that sends message to kafka
                                   timestamp_ms=int(datetime.strptime(message['publishedAt'], "%Y-%m-%d %H:%M:%S").timestamp() * 1000))
            future.get(timeout=10) # waiting up to 10 secs to find out if the message has actually been sent
            print(f"Sent: {message['title'][:50]}...")
            return True
        except Exception as e:
            if attempt == 2: # Here we are trying 3 times in case any error occurs
                print(f"Final Send Failure: {str(e)}")
                return False
            print(f"Retry {attempt+1}/3: {str(e)}")
            time.sleep(5) # waits for 5 sec before trying again


def verify_kafka_connection():
    for _ in range(5):  # 5 retries
        try: # If there is a difficulty in connection trying multiple times could solve the problem
            producer.send(TOPIC_NAME, {"test": "connection"}).get(timeout=10) # its just a test message to find out if kafka is working waiting for 10 sec  for teh response
            return True # Test message ws sent
        except Exception as e:
            print(f"Kafka connection failed: {e}") # show the error to know what went wrong
            time.sleep(5)
    return False

if not verify_kafka_connection():
    sys.exit(1) # run the test to know if kafka is working if not no point in continuing

# ---- MAIN WORKFLOW ----
# Below is the main code that executed every defined functioned above
def main_loop():
    while True: # starts an infinite loop
        try:
            print("\n=== Starting news fetch ===")
            articles = fetch_articles() # calls the function that gets article
            if articles is None:
                return False # if something went wrong stop the loop 
            
            if not articles:
                print("No articles fetched")
                return True # if articles is empty  return True, program is okay - just no news this time
            # Here message deduplication has been used so as to enter unique entries only
            unique_articles = {} # creates an empty dictionary to remove duplicate articles
            for article in articles: #goes through each article and gets title
                title = article.get('title', '')
                if title not in unique_articles: # if apriori the article is not present in the dictionary
                    unique_articles[title] = article # add the title to dictionary
            articles = list(unique_articles.values()) # replace the original list with the unique_articles
            print(f"Reduced duplicated articles: {len(articles)}")
                
            success_count = 0 # counter for how many message have been successfully sent to kafka
            for article in articles:
                message = process_article(article) # it is used to clean and process the news fetched
                if message and send_message(message): # if the articles is avlid and has been sent successfully add 1
                    success_count += 1  # Flush the Kafka producer to immediately send any buffered messages.
                if success_count % 10 == 0: # This ensures that all messages are delivered, even if the batch size is not yet full.
                    producer.flush()
            producer.flush()
            print(f"=== Batch complete: {success_count}/{len(articles)} sent ===") # shows how many articles were sent 
            return True
            
        except Exception as e:
            print(f"Error: {str(e)}")
            return False # if error occurs turn false

if __name__ == "__main__": # this runs only when the script is started directly
    try:
        while True: # Starts an infinite loop
            success = main_loop() # Stores the main loop function in success variable
            wait_time = 3600 # waits for 1 hour for the next fetch
            message = "Waiting 1 hour before next fetch..."
            
            if not success:
                wait_time = 3600
                message = "Waiting 1 hour due to API limits..."
            
            print(f"\n{message}")
            time.sleep(wait_time)
            
    except KeyboardInterrupt: # Just to forcefully shutdown the producer
        print("\nShutting down producer...")
        producer.close()
        sys.exit(0)