# spark_job.py  (rename file if you like)
import os, json
from kafka import KafkaConsumer
from pymongo import MongoClient

TOPIC = os.getenv("TOPIC", "yt-comments")                 # <-- use env / same as dashboard
BROKER = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="comment_workers",
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),  # decode here
)

mongo = MongoClient(MONGO_URI)
coll = mongo["youtube"]["comments"]

print(f"Worker started. Subscribed to topic: {TOPIC}")

for msg in consumer:
    try:
        data = msg.value  # already dict due to value_deserializer
        # basic fields
        doc = {
            "video_id": data.get("video_id"),
            "comment_id": data.get("comment_id"),
            "author": data.get("author"),
            "text": data.get("text"),
            "likeCount": data.get("likeCount"),
            "publishedAt": data.get("publishedAt"),
            "sentiment": data.get("sentiment", {}),
        }
        coll.update_one({"comment_id": doc["comment_id"]}, {"$set": doc}, upsert=True)
        print("Saved:", doc["comment_id"], doc.get("sentiment", {}).get("label"))
    except Exception as e:
        print("Worker error:", e)
