# Real-Time Financial News Sentiment Engine

## Project Overview
This project implements a **real-time financial news sentiment analysis system** that helps investors and traders quickly understand market sentiment. The system fetches financial news using the **News API**, processes it in real time using **Apache Kafka** and **Apache Spark**, and displays the results in an interactive **Streamlit dashboard**. Additionally, all processed data is saved for historical analysis.

---

## Architecture & Workflow
1. **News Fetching (Producer)**  
   - The producer continuously fetches financial news using the News API.  
   - News is sent to a **Kafka topic** for streaming.

2. **Real-Time Processing (Spark)**  
   - Spark reads news from the Kafka topic in real time.  
   - Sentiment analysis is performed using the **VADER model**.  
   - The processed data is sent to both the **Streamlit dashboard** for visualization and to the **consumer** for saving.

3. **Data Storage (Consumer)**  
   - The consumer saves processed news data into a **CSV file** for historical analysis.

4. **Visualization (Streamlit Dashboard)**  
   - Displays the latest news along with:  
     - News link for reading full articles  
     - Source and publication date  
     - Sentiment label (positive, negative, neutral)  
     - Sentiment score  
   - Interactive filters to view news by **source** or **sentiment**  
   - Graphs and charts for overall sentiment trends

---

## Key Features
- **Real-Time Streaming:** Uses Kafka and Spark to handle live news updates  
- **Sentiment Analysis:** VADER model to classify sentiment and assign scores  
- **Interactive Dashboard:** Streamlit interface for filtering, reading, and visualizing news  
- **Data Logging:** Consumer saves processed data for future analysis  
- **Filtering & Analytics:** Filter news by source or sentiment, track trends with charts  

---

## Files in Repository
- `producer.py` → script of Producer  
- `Dockerfile` → Producer Dockerfile  
- `Spark.py` → script of Spark 
- `Dockerfile` → Spark Dockerfile  
- `start_spark.sh` → Spark Start file
- `consumer.py` → script of Consumer
- `Dockerfile` → Consumer Dockerfile
- `dashboard.py` → script of Dashboard
- `Dockerfile` → Dashboard Dockerfile
- `docker_compose.yaml` → Docker compose file

---

## Technologies Used
- **Python:** Pandas, NumPy, NLTK, VADER  
- **Real-Time Streaming:** Apache Kafka, Apache Spark  
- **Containerization:** Docker  
- **Dashboard & Visualization:** Streamlit, Matplotlib, Seaborn  

---

## How to Run
1. Clone the repository:  
```bash
git clone https://github.com/MohdKaif-byte/Real-Time-Financial-News-Sentiment-Engine.git

2, Install dependencies:

pip install -r requirements.txt


