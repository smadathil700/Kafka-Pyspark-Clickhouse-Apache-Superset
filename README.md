# Reddit Reactions to Trump: A Sentiment Analysis

This project performs a real-time sentiment analysis of Reddit comments mentioning Donald Trump using Apache Kafka, PySpark, TextBlob, ClickHouse, and Grafana. The goal is to stream comments, analyze their sentiment, and visualize the results on a live dashboard.

## 🛠️ Features

- **Real-time Reddit comment ingestion** using Kafka
- **Sentiment analysis** using TextBlob in a PySpark environment
- **Data pipeline and processing** in Jupyter Notebook via VS Code
- **Storage** of processed data in ClickHouse
- **Live data visualization** in Grafana

## 🧰 Tech Stack

- **Reddit API** (praw)
- **Apache Kafka & Zookeeper** (via Docker)
- **PySpark** in Jupyter Notebook (via Docker)
- **ClickHouse Server** (via Docker)
- **Grafana** (via Docker)
- **Python Libraries**: praw, kafka-python, textblob, clickhouse-connect

## 📦 Setup & Configuration

### 1. Reddit API
- Created API credentials (Client ID, Secret, and User Agent) using [Reddit Developer Portal](https://www.reddit.com/prefs/apps)
- Used `praw` for API integration

### 2. Kafka and Zookeeper (Docker)
```bash
docker-compose up -d kafka zookeeper
```

### 3. Jupyter PySpark Notebook (Docker)
- Launched and connected via Visual Studio Code using Jupyter Server kernel

### 4. ClickHouse Server (Docker)
```bash
docker run -d --name clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server
```

### 5. Grafana (Docker)
```bash
docker run -d -p 3000:3000 grafana/grafana
```

### Ensure to add all of them in one network. Example script below:
```bash
docker network connect analytics-net grafana
```
_analytics-net is name of network_

## 🔄 Data Pipeline Workflow

1. **Kafka Producer**: Streams all Reddit comments mentioning "Trump" into a Kafka topic
2. **Sentiment Analysis**: 
   - Spark session created with a defined schema
   - Sentiment of each comment analyzed using TextBlob
3. **Storage**: 
   - Results written to local Parquet files via Jupyter
   - Transformed data loaded into ClickHouse (`reddit_cleaned` table in default database)
4. **Visualization**: 
   - ClickHouse used as a data source in Grafana
   - Interactive dashboard created to visualize comment sentiment over time

## 📊 Dashboard Example (Grafana)

- **Real-time trend** of sentiment scores
- **Volume** of positive vs negative mentions
- **Time-series analysis** of comment sentiment

## 📁 Output Structure

### Parquet Files
- Local output from Spark

### ClickHouse Table
- **Table**: `reddit_cleaned`
- **Fields include**: comment_id, body, sentiment, timestamp, etc.

## 🚀 Getting Started

1. Make sure you have **Docker** and **VS Code** installed
2. Clone this repo and run the following:

```bash
# Start Docker containers for Kafka, Zookeeper, Jupyter, ClickHouse, and Grafana
docker-compose up
```

3. Ensure your `.env` or config file includes Reddit API credentials and relevant topic names

## 🤝 Contributing

Pull requests are welcome. For major changes, please open an issue first.



## 📋 Prerequisites

- Docker & Docker Compose
- Visual Studio Code
- Reddit Developer Account
- Basic knowledge of Python and data engineering concepts

## 🔧 Configuration Files

### Docker Compose
Ensure your `docker-compose.yml` includes all required services:
- Kafka
- Zookeeper
- Jupyter/PySpark
- ClickHouse
- Grafana

## 🎯 Project Goals

- Demonstrate real-time data processing capabilities
- Create scalable data pipeline architecture
- Provide interactive data visualization

