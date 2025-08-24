import pandas as pd
import streamlit as st
from streamlit_autorefresh import st_autorefresh
import datetime
import os
from kafka import KafkaConsumer
import json

def load_latest_kafka_data(): # This function reads the latest messages from kafka topic called financial-news-sentiment
    try:
        consumer = KafkaConsumer(# initialize kafka consumer that reads the processed data
            'financial-news-sentiment', # This is topic from where we wann read
            bootstrap_servers='kafka:9092', # this is the address of the kafka
            auto_offset_reset='earliest', # start reading the messages from starting
            group_id = "dashboard_consumer", # This keeps track of what has been read if something bad happens it keeps tracj to start from there itself
            enable_auto_commit=True, #kafka would automatically mark messages as read
            consumer_timeout_ms = 10000, # stop listening after 10 secs if no message comes
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) # converts the message from JSON to python dictionary
        )
        data = [] # this is the empty list to store the data
        for i, message in enumerate(consumer): # go through every message one by one
            article = message.value # this gives the actual news content
            required_keys = ['title', 'source', 'publishedAt', 'sentiment', 'confidence', 'link']
            if not all(k in article for k in required_keys): # make sure the above columns or the information is present if not
                continue  # skip incomplete messages safely
            data.append({
                'title': article['title'],
                'source': article['source'],
                'publishedAt': article['publishedAt'],
                'sentiment': article['sentiment'].capitalize(),
                'confidence': round(float(article['confidence']), 3),
                'link': article['link']
            }) # store the clean and formatted data in our list
            if i + 1 >= 31: # since only 30 news is getting fetched at once this condition could be applied
                break
        return pd.DataFrame(data).sort_values(['publishedAt'], ascending=[True]) # return th epandas dataframe sorted by time of news published
    except Exception as e:
        st.error(f"Kafka read error: {e}")
        return pd.DataFrame() # if something goes wrong show the error and retuen empty data

def reset_csv_daily(): # This has been done so everyday fresh data gets loaded to the csv file for the historical analysis
    today_str = datetime.date.today().isoformat() # get todays data like "2025-08-21"
    empty_df = pd.DataFrame(columns=['title', 'sentiment', 'source', 'publishedAt', 'confidence', 'link']) # makes an empty dataframe with correct columns 
    if not os.path.exists("/app/shared_data/last_reset.txt"): # checks if the file that stores last reset date exists
        with open("/app/shared_data/last_reset.txt", 'w') as f:
            f.write(today_str) # if it is first time running create the file and reset the CSV
        empty_df.to_csv("/app/shared_data/sentiment_data.csv", index=False)
        return
    with open("/app/shared_data/last_reset.txt", 'r') as f:
        last_reset = f.read().strip()
    if last_reset != today_str:
        empty_df.to_csv("/app/shared_data/sentiment_data.csv", index=False)
        with open("/app/shared_data/last_reset.txt", 'w') as f:
            f.write(today_str) # if todaay is the new day, we wipe the CSV and update the reset file

reset_csv_daily()

st.set_page_config(page_title="Financial Sentiment Dashboard", layout="wide")
st.title("Financial News Sentiment Dashboard") # Setting the title of the dashboard and the layout

count = st_autorefresh(interval=3600000, limit=None, key="refresh")
placeholder = st.empty() # refreshing every hour to get retrieve fresh news every hour

@st.cache_data(ttl=3600) #load the csv data and keep it in memory for 1 hour
def load_data():
    try:
        df = pd.read_csv("/app/shared_data/sentiment_data.csv")
        if not df.empty:
            df['sentiment'] = df['sentiment'].str.capitalize()
            df = df.sort_values(['publishedAt'], ascending=[True]) # clean data if its not empty capitalize teh sentiment and arrange it by time
        return df
    except FileNotFoundError:
        return pd.DataFrame() # if no file found return the empty data

# Load latest batch data from Spark
latest_data = load_latest_kafka_data()

# Load historical data from CSV
historical_data = load_data()

# Converting 'publishedAt' to datetime for proper sorting/filtering
if not latest_data.empty:
    latest_data['publishedAt'] = pd.to_datetime(latest_data['publishedAt'], errors='coerce')
if not historical_data.empty:
    historical_data['publishedAt'] = pd.to_datetime(historical_data['publishedAt'], errors='coerce')


if latest_data.empty and historical_data.empty: # message would be displayed if there is no data available
    st.warning("No sentiment data available yet.")
    st.stop()

with placeholder.container(): # this tells in the dashboard when is the last time the data is updated
    st.caption(f"Last updated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


# --- Historical Sentiment ---
if not historical_data.empty: # if historical data is present
    st.subheader("Historical Sentiment Distribution") # tthis adds small title hostorical sentiment distribution
    all_sentiments = ['Positive', 'Negative', 'Neutral'] # these are expected sentiment categories
    hist_sentiment_counts = historical_data['sentiment'].value_counts().reindex(all_sentiments, fill_value=0) # counts how many times each sentiment appears and if any sentiment is missing 
    col1, col2 = st.columns([1, 2])# it will be filled with 0
    with col1:# the above line of code splits the screen into 2 parts one narrow and one wide
        st.write("Total Counts:", hist_sentiment_counts) # show the actual count numbers in the first columns
    with col2:
        st.bar_chart(hist_sentiment_counts) #show the bar chart of the historical sentiment distribution in the second column

    # --- Add filters for Historical Headlines ---
    st.subheader('Filter Historical Headlines') # This is the title for filtering section where user can filter news headlines
    col1_hist, col2_hist = st.columns(2) # split the section into 2 equal columns  for 2 filters
    with col1_hist:
        hist_sentiment_filter = st.selectbox( # in the first column the drop down lets the person choose
            "Filter by Sentiment (Historical)", # which historical news they wanna see based on the sentiment
            options=['All'] + all_sentiments, 
            key="hist_sentiment_filter"
        )
    with col2_hist:
        hist_source_filter = st.multiselect( # in this second column users can choose fromm which source they wanna see
            "Filter by News Source (Historical)", # the historical news, by default all news sources are selected
            options=historical_data['source'].unique(),
            default=historical_data['source'].unique(),
            key="hist_source_filter"
        )

    filtered_historical = historical_data.copy() # here the copy of data has been made to apply filters without changing the original data
    if hist_sentiment_filter != 'All': #if user selct specific sentiment keep the rows with that sentiment only
        filtered_historical = filtered_historical[filtered_historical['sentiment'] == hist_sentiment_filter]
    if hist_source_filter:
        filtered_historical = filtered_historical[filtered_historical['source'].isin(hist_source_filter)] # if the user has selected only a specific source keep that only

    st.subheader("Historical Headlines") # add a title for the display of historical headlines
    st.write(f"Showing {len(filtered_historical)} historical headlines:") # show how many headlines match the filter
    for _, row in filtered_historical.sort_values('publishedAt', ascending=False).iterrows(): # go through each row sorted from latest to oldest 
        st.markdown( # this displays 
            f"**{row['title']}**  \n" # title in bold
            f"_Source_: {row['source']}  \n" #source in italic
            f"_Published at_: {row['publishedAt']}  \n" # date in italic
            f"_Sentiment_: {row['sentiment']} ({row['confidence']:.2f})  \n" # displays the sentiment and confidence score
            f"[Link to article]({row['link']})", # this is the link where a user can directly go to read the news
            unsafe_allow_html=True
        )
        st.markdown("---") # this is a line separator to keep the dashboard clean
else:
    st.info("No historical sentiment data available.") #If the CSV had no data at all, display a message saying there’s nothing to show.


# --- Latest Batch Sentiment ---
if not latest_data.empty: # this checks if the latest data from kafka exists. if yes proceed to analyze
    latest_batch_data = latest_data # just storing the latest data into another variable for clarity
    st.subheader(f"Latest Batch Analysis") #Adds a section title on the Streamlit dashboard: "Latest Batch Analysis"
    all_sentiments = ['Positive', 'Negative', 'Neutral'] # this is the list of all possible sentiment label
    latest_sentiment_counts = latest_batch_data['sentiment'].value_counts().reindex(all_sentiments, fill_value=0) # counts how many news items belong to each sentiment
    st.bar_chart(latest_sentiment_counts) # draw a bar chart showing how many news items fall in which sentiment category

    # Filters for latest batch headlines - allows filtering by different techniques
    st.subheader('Filter Latest Batch Headlines') # add subtitle of filter latest batch
    col1, col2 = st.columns(2) # split the page into 2 columns one for sentiment filteer and otehr for source filter
    with col1: # in first column a drop down menu is shown to filter by sentiment
        sentiment_filter = st.selectbox("Filter by Sentiment", options=['All'] + all_sentiments, key="latest_sentiment_filter")
    with col2: # in this column which 2nd it allows multi-select drop down let suser filter from multiple news sources
        source_filter = st.multiselect(
            "Filter by News Source",
            options=latest_batch_data['source'].unique(),
            default=latest_batch_data['source'].unique(),
            key="latest_source_filter"
        )

    filtered_latest = latest_batch_data.copy() # make a copy of latest news to apply filters on it 
    if sentiment_filter != 'All': # if sepecific filter is selected show news data of that filter only not all
        filtered_latest = filtered_latest[filtered_latest['sentiment'] == sentiment_filter]
    if source_filter: #If some news sources were selected, filter the data to show only those sources.
        filtered_latest = filtered_latest[filtered_latest['source'].isin(source_filter)]
# Following code shows the news headlines with all other information like how many news headlines are there
    st.write(f"Showing {len(filtered_latest)} latest headlines:")
    for _, row in filtered_latest.sort_values('publishedAt', ascending=False).iterrows(): # go through each news data sorted by by newest first
        st.markdown( # for each title display
            f"**{row['title']}**  \n" # title in bold
            f"_Source_: {row['source']}  \n" # source in italics
            f"_Published at_: {row['publishedAt']}  \n" #published in italics
            f"_Sentiment_: {row['sentiment']} ({row['confidence']:.2f}) \n" # sentiment and confidence score together
            f"[Link to article]({row['link']})", # a clicakble link to view full article
            unsafe_allow_html=True
        )
        st.markdown("---") # an horizontal line between each news headline for cleanliness
else:
    st.info("No latest batch data available.") # If there was no data available from Kafka, show message



