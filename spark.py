import os
import json
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, from_json, to_json, struct, from_unixtime, to_timestamp, expr, window
from pyspark.sql.types import StringType, FloatType, StructType, StructField, TimestampType, DoubleType
from pyspark.sql.functions import to_json, struct, when
from pyspark.sql.functions import from_json as from_json_func
import shutil

# Delete existing checkpoint directories to prevent Spark from restoring previous streaming state.
# This ensures the job starts fresh each time the script is run.
shutil.rmtree("/tmp/financial_news_sentiment_checkpoint", ignore_errors=True)
shutil.rmtree("/tmp/parquet_checkpoint", ignore_errors=True)
# ---- Load VADER Model once globally ----
print("Loading VADER model ...")
analyzer = SentimentIntensityAnalyzer() # This line loads the VADER model once at the starting so it can be used to find the sentiment of the news headlines

# ---- Define UDF for sentiment analysis ----
def analyze_sentiment(text):
    try:
        vs = analyzer.polarity_scores(text) # This line takes the text and inputs into the VADER model to get the sentiment and score
        compound = vs['compound'] # It takes the overall mood to get the total feeling of teh sentence
        # Simple logic to map compound score to label
        if compound >= 0.05:
            label = "Positive"
        elif compound <= -0.05:
            label = "Negative"
        else:
            label = "Neutral"
        return label, float(compound)
    except Exception as e: # if there is an error return Neutral with a score of 0.0 score for safe fallback
        return 'Neutral', 0.0

def sentiment_udf(text): # it is a kind of helper function which calls analyze sentiment function and formats result as JSON
    if not text:
        return json.dumps({"sentiment": "Neutral", "confidence": 0.0}) # if the text is empty don not analyze it
    label, score = analyze_sentiment(text)
    return json.dumps({"sentiment": label, "confidence": score}) # it says otherwise analyzeit and pack it in JSON-likne format

sentiment_analysis_udf = udf(sentiment_udf, StringType()) # udf is a built-in function so what it does is that it let spark know that i made a custom function. it takes soem text and  gives me string output the JSON

# ---- Define schema for incoming Kafka JSON messages ----
news_schema = StructType([
    StructField("title", StringType(), True), # this is the shape of the  each news item - it has a title, spurce, time, link, and when we fetched it
    StructField("source", StringType(), True),
    StructField("publishedAt", StringType(), True),
    StructField("link", StringType(), True),
    StructField("fetched_at", DoubleType(), True)  # from time.time()
])

# ---- Initialize Spark Session ---- # Now we start the spark session so it can process the data using all the above tools
spark = SparkSession.builder \
    .appName("FinancialNewsSentimentStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.driver.host", "spark") \
    .config("spark.cleaner.referenceTracking.cleanCheckpoints", "true") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .getOrCreate() # The app has been named which is good for debugging and logs
#Run spark locally using all cpu cores
#Loading kafka package so spark can talk to kafka
#says to use  only 1 partition
#sets the name of the spark's main driveer to that the containers can talk to each other
#helps spark automatically clean up old checkpoint info. keeps the app clean
# Tells spark to use local file system and not something like HDFS(ehich is used in big clusters)
#finally  start or reuse the spark session

spark.sparkContext.setLogLevel("WARN") # This says just to show the warning and errors and do not fill it with too much information

# ---- Read from Kafka topic 'financial-news' ----
raw_stream_df = (
    spark.readStream.format("kafka") # This says to read stream of data and it comes from kafka
    .option("kafka.bootstrap.servers", "kafka:9092") # This is the address to which kafka connects to read the news
    .option("subscribe", "financial-news") # subscribing to this topic from where the news will come
    .option("startingOffsets", "earliest") # start reading from the beginning of topi and not just the new data
    .load() # finally the raw_stream_df holds the stream of incoming news articles from kafka 
)

# ---- ADD DEBUGGING HERE ----
raw_stream_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .option("numRows", 5) \
    .start()
# This is just for debugging it takes teh stream of data from kafka which is in bytes and converts both key and value 
# into readable text print 5 rows on the terminal just sees if te data is actually coming and to see what it looks like

# ---- Parse Kafka messages ----
news_df = raw_stream_df.selectExpr("CAST(value AS STRING) as json_str")\
    .select(from_json(col("json_str"), news_schema).alias("data"))\
    .select("data.*")
# it Reads the kafka message and calls it json_str and passes the JSON from the schema defined above
# ---- Convert fetched_at to timestamp and apply watermark ----
news_df = news_df.withColumn("fetched_at_ts", to_timestamp(from_unixtime(col("fetched_at")))) #Takes the time (which is in numbers like 1719576200) and turns it into a readable date-time (like 2024-06-28 12:00:00).

# Apply watermark of 10 minutes
news_df_with_watermark = news_df.withWatermark("fetched_at_ts", "10 minutes") # says that keep only the news from the last 10 mins anything older won't be considered

# ---- Apply sentiment analysis UDF on title ----
sentiment_json_col = sentiment_analysis_udf(col("title")) # Takes the tilte of te news article and runs the sentiment function with VADER and gets the result

sentiment_struct_schema = StructType([
    StructField("sentiment", StringType()),
    StructField("confidence", FloatType())
]) # Tells spark that result from sentiment function will have 2 parts  - sentiment - a text and confidence - a number

news_df_with_sentiment = news_df_with_watermark.withColumn("sentiment_json", sentiment_json_col) # Adds a new column to the table called sentimnt_json which contains teh result from VADER
# Debug: Show bad or empty sentiment_json values to find root cause
bad_json_query = news_df_with_sentiment \
    .filter(col("sentiment_json").isNull() | (col("sentiment_json") == "")) \
    .select("title", "sentiment_json") \
    .writeStream \
    .format("console") \
    .option("truncate", False) \
    .option("numRows", 50) \
    .start()
# writetostream says start streaming the data
#format(console) says print it on my terminal screen
#option(numrows, 50) says shows only 50 rows
#option(truncate, false) says do not cut the long string print it full
# start() says finally start the process
#filters and prints only those rows where the sentiment result is missing or empty to spot the problems like VADER failed or title is missing 

debug_query = news_df_with_sentiment.select("title", "sentiment_json").writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()
# Prints the title and sentiment result of each article

news_df_with_sentiment = news_df_with_sentiment.withColumn(
    "sentiment_struct", # This creates a new column called sentiemnt_struct
    when(
        col("sentiment_json").isNotNull() & (col("sentiment_json") != ""),
        from_json_func(col("sentiment_json"), sentiment_struct_schema)
    ).otherwise(None)
)
# looks at the sentiment_json column and if its not empty turn it into two columns sentiment being positive negative or neutral
# and confidence of teh sentiment which would be a number
final_df = news_df_with_sentiment.select(
    "title", "source", "publishedAt", "link", "fetched_at",
    col("sentiment_struct.sentiment").alias("sentiment"),
    col("sentiment_struct.confidence").alias("confidence"),
    col("fetched_at_ts") # this keeps the final column we want to have and gives names to columns and finaly th etimestamp we added earlier
)

# Updating the struct to use consistent naming
output_df = final_df.select(
    to_json(struct( # spark can only send key value pairs so we here combine all columns into one json object using struct() and then turn everything to json
        col("title").alias("title"),
        col("source").alias("source"),
        col("publishedAt").alias("publishedAt"),
        col("link").alias("link"),
        col("fetched_at").alias("fetched_at"),
        col("sentiment").alias("sentiment"),
        col("confidence").alias("confidence"),
        col("fetched_at_ts").alias("fetched_at_ts")
    )).alias("value")
)

#  Adding debugging here 
print("Final DataFrame schema:")
final_df.printSchema() # Just prints the structure of final_df to the terminal. This helps you see what columns are in the final result and their types.


# Write processed data to Kafka topic 'financial-news-sentiment'
kafka_query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "financial-news-sentiment") \
    .option("checkpointLocation", "/tmp/financial_news_sentiment_checkpoint") \
    .option("failOnDataLoss", "false") \
    .start()
# Now here we are sending the data to another kafka topic financial-news-sentiment it sends data to kafka let knows spark where is kafka and then which topic to sent and let spark
#know where it left in case of restarts using checkpointlocation, and i fsome data is missing spark won't crash and then finally start the job
print("All streams started:")
for q in spark.streams.active:
    print(f"  {q.name}: {q.status['message']}") # This let me know all the streaming jobs active and check if the streaming has started successfully

spark.streams.awaitAnyTermination() # Keep the application running forever and wait for news data without this the program would immideately stop



