#  YouTube Sentiment Analysis – Kafka + Mongo + Streamlit

A simple **Big Data pipeline** project for **real-time YouTube comment sentiment analysis**.  
It demonstrates how to integrate **YouTube API**, **Kafka**, **MongoDB**, and **Streamlit** in a containerized environment using **Docker Compose**.


##  Overview

**Workflow:**

1. **Streamlit Dashboard**
   - User enters a **YouTube video ID**.
   - Fetches latest comments via the **YouTube Data API**.
   - Performs **sentiment analysis** using **VADER**.
   - Sends each comment (JSON) to **Kafka** topic (`yt-comments`).

2. **Kafka Worker (Python Consumer)**
   - Listens to the `yt-comments` topic.
   - Parses messages and stores them into **MongoDB**.
   - Each record includes `video_id`, `author`, `text`, `sentiment`, etc.

3. **Dashboard Visualization**
   - Reads processed comments directly from MongoDB.
   - Displays charts for positive / neutral / negative distribution.
   - Allows pagination, sorting (Newest/Oldest), and filtering by sentiment.


##  Project Structure

📦 youtube-sentiment-bigdata
├── dashboard/
│ ├── app.py
│ └── requirements.txt
├── spark/ # (Worker folder)
│ └── spark_job.py
├── docker-compose.yaml
└── README.md



## 🧰 Tools & Technologies

| Component  | Description |
|-------------|-------------|
| **Python 3.11** | Used for both dashboard & worker logic |
| **Streamlit** | Web frontend for analysis & visualization |
| **Kafka (Confluent 7.6)** | Message broker for streaming comments |
| **MongoDB 6.0** | NoSQL storage for processed comments |
| **VADER** | Lexicon-based sentiment analyzer |
| **Docker Compose** | Orchestrates all services together |


## ⚙️ Setup Instructions

### 1️⃣ Prerequisites
- Docker & Docker Compose installed  
- Valid [YouTube Data API v3](https://console.cloud.google.com/apis/library/youtube.googleapis.com) key

### 2️⃣ Set API Key

#### PowerShell (Windows)

bash
$env:YT_API_KEY="YOUR_YOUTUBE_DATA_API_KEY"
macOS/Linux
bash
Copy code
export YT_API_KEY="YOUR_YOUTUBE_DATA_API_KEY"
 Run the Project
Step 1: Start core services
bash:

    docker compose down -v --remove-orphans
    docker compose pull
    docker compose up -d zookeeper kafka mongo

Step 2: Create Kafka topic (safe to re-run)
bash
    docker compose exec kafka bash -lc "/usr/bin/kafka-topics --create --if-not-exists --topic yt-comments --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1"

Step 3: Start worker & dashboard
bash
    docker compose up -d worker dashboard

🌐 Access the Dashboard

Open your browser and go to:
👉 http://localhost:8501

Enter a YouTube Video ID (e.g., dQw4w9WgXcQ)

Click Fetch & Produce

Wait a few seconds for worker to insert comments into MongoDB

Use filters to explore positive / neutral / negative comments

🧾 Example Flow

User enters video ID → Streamlit fetches comments → sends to Kafka →
Worker consumes → stores in MongoDB →
Dashboard visualizes sentiment stats
🧩 Useful Commands
Check logs:

bash

docker compose logs dashboard --since=2m | tail -n 50
docker compose logs worker --since=2m | tail -n 50
Check MongoDB count:

bash

docker compose exec mongo mongosh --quiet --eval "db.getSiblingDB('youtube').comments.count({})"
Consume Kafka topic manually:

bash

docker compose exec kafka bash -lc "/usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic yt-comments --from-beginning --timeout-ms 10000 | head -n 5"


docker compose down -v




# To Run 

docker compose down -v --remove-orphans
docker compose pull
docker compose up -d zookeeper kafka mongo

docker compose exec kafka bash -lc "/usr/bin/kafka-topics --create --if-not-exists --topic yt-comments --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1"


docker compose up -d worker dashboard
