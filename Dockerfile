FROM python:3.9-slim-bullseye

WORKDIR /app

# Single RUN command with proper line continuation
RUN apt-get update && \
    apt-get install -y openjdk-11-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
RUN pip install --no-cache-dir \
    pandas \
    streamlit \
    matplotlib \
    streamlit-autorefresh \
    pyarrow \
    kafka-python 

COPY dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py", "--server.port=8501", "--server.address=0.0.0.0"]
