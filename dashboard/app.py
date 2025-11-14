import os, requests, pandas as pd, streamlit as st, altair as alt
from dateutil import parser as dt
from pymongo import MongoClient, ASCENDING, DESCENDING
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaTimeoutError
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
API_KEY = os.getenv("YT_API_KEY", "")
DB, COLL = "youtube", "comments"
TOPIC = "yt-comments"
FETCH_LIMIT = 2000
PAGE_SIZE_DEFAULT = 50

st.set_page_config(page_title="YouTube Sentiment – Streaming", layout="wide")
st.title("📺 YouTube Sentiment – On-demand → Kafka → Spark → Mongo")

# DB + indexes
client = MongoClient(MONGO_URI)
coll = client[DB][COLL]
coll.create_index([("video_id", ASCENDING), ("publishedAt", DESCENDING)])
coll.create_index([("video_id", ASCENDING), ("sentiment.label", ASCENDING)])

# Kafka producer 
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: __import__("json").dumps(v).encode("utf-8"),
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=10000,
    )
    kafka_ok = True
except NoBrokersAvailable:
    producer = None
    kafka_ok = False

analyzer = SentimentIntensityAnalyzer()

def score(txt:str):
    s = analyzer.polarity_scores(txt or "")
    c = s["compound"]
    return {"label": "positive" if c>=0.05 else ("negative" if c<=-0.05 else "neutral"), "compound": c}

API_URL = "https://www.googleapis.com/youtube/v3/commentThreads"

def fetch_comments(video_id:str, cap:int=FETCH_LIMIT):
    params = {
        "part":"snippet", "videoId":video_id, "maxResults":100,
        "order":"time", "textFormat":"plainText", "key":API_KEY
    }
    seen, next_page = 0, None
    while True:
        if next_page: params["pageToken"] = next_page
        r = requests.get(API_URL, params=params, timeout=20); r.raise_for_status()
        data = r.json(); items = data.get("items", [])
        if not items: break
        for it in items:
            s = it["snippet"]["topLevelComment"]["snippet"]
            yield {
                "video_id": video_id,
                "comment_id": it["snippet"]["topLevelComment"]["id"],
                "author": s.get("authorDisplayName"),
                "text": s.get("textDisplay",""),
                "likeCount": int(s.get("likeCount",0)),
                "publishedAt": s.get("publishedAt")
            }
            seen += 1
            if seen >= cap: return
        next_page = data.get("nextPageToken")
        if not next_page: break

with st.sidebar:
    st.header("Fetch to Kafka")
    vid = st.text_input("YouTube Video ID", placeholder="e.g., dQw4w9WgXcQ")
    cap = st.number_input("Max comments", 50, 5000, FETCH_LIMIT, step=50)
    do_fetch = st.button("Fetch & Produce")
    st.caption(f"API key: {'✅ set' if API_KEY else '❌ missing'} / Kafka: {'✅' if kafka_ok else '❌'}")

if do_fetch:
    if not API_KEY:
        st.error("Set YT_API_KEY in your environment.")
    elif not kafka_ok:
        st.error("Kafka broker not reachable. Is 'kafka' container up?"); st.stop()
    elif not vid.strip():
        st.error("Enter a video ID.")
    else:
        st.info("Fetching → scoring → producing to Kafka…")
        cnt = 0
        for c in fetch_comments(vid.strip(), int(cap)):
            c["sentiment"] = score(c["text"])
            try:
                c["publishedAt"] = dt.parse(c["publishedAt"]).isoformat()
            except: pass
            try:
                producer.send(TOPIC, c).get(timeout=10)  # wait for ack
                cnt += 1
            except KafkaTimeoutError as e:
                st.error(f"Kafka timeout: {e}"); st.stop()
        producer.flush()
        st.success(f"Produced {cnt} messages to Kafka topic `{TOPIC}`. Spark will write them to Mongo.")

st.header("Explore (from Mongo)")
q_video = st.text_input("Filter by video ID", value=vid or "")
colA, colB, colC = st.columns(3)
with colA: label = st.selectbox("Sentiment", ["all","positive","neutral","negative"])
with colB: order = st.selectbox("Order", ["Newest","Oldest"])
with colC: page_size = st.number_input("Page size", 20, 200, PAGE_SIZE_DEFAULT, step=10)

if "offset" not in st.session_state: st.session_state.offset = 0
if st.button("Reset pagination"): st.session_state.offset = 0

query = {}
if q_video.strip(): query["video_id"] = q_video.strip()
if label != "all": query["sentiment.label"] = label
sort = [("publishedAt", -1 if order=="Newest" else 1)]

# chart data (counts)
agg = list(coll.aggregate([{"$match": query},{"$group":{"_id":"$sentiment.label","count":{"$sum":1}}}]))
chart_df = pd.DataFrame({"label":[a["_id"] or "unknown" for a in agg],
                         "count":[a["count"] for a in agg]})

left, right = st.columns(2)
with left:
    st.subheader("Sentiment distribution")
    if not chart_df.empty:
        st.altair_chart(alt.Chart(chart_df).mark_bar().encode(x="label:N", y="count:Q"),
                        use_container_width=True)
    else:
        st.info("No data yet. Fetch & Produce first, then wait for Spark to write.")
with right:
    st.subheader("Counts")
    st.metric("Total", int(chart_df["count"].sum()) if not chart_df.empty else 0)

# page of rows
rows = list(coll.find(query, {"_id":0}).sort(sort)
            .skip(st.session_state.offset).limit(int(page_size)))
if rows:
    df = pd.DataFrame(rows)
    st.subheader("Comments (page)")
    st.dataframe(df[["publishedAt","video_id","author","sentiment","text","likeCount"]],
                 use_container_width=True, hide_index=True)
    c1,c2,c3 = st.columns(3)
    with c1:
        if st.session_state.offset>0 and st.button("⬅️ Prev"):
            st.session_state.offset = max(0, st.session_state.offset-int(page_size)); st.rerun()
    with c2: st.write(f"Offset: {st.session_state.offset}")
    with c3:
        if len(rows)==int(page_size) and st.button("Next ➡️"):
            st.session_state.offset += int(page_size); st.rerun()
else:
    st.info("No rows for current filters/page.")
