import praw
from kafka import KafkaProducer
import json
import time

# --- Reddit API Setup ---
reddit = praw.Reddit(
    client_id='m3f5BiQVY_4rxw0KFAeQsw',
    client_secret='vwvxfVk0LJp0lkui39iryOE34Ducnw',
    user_agent='TrumpDiscussionAnalyzer/0.1 by sariga madathil'
)

# --- Kafka Producer Setup ---
producer = KafkaProducer(
    bootstrap_servers='host.docker.internal:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# --- Kafka Topic ---
kafka_topic = 'reddit-comments-trump'

# --- Stream Reddit Comments Mentioning "Trump" ---
def stream_trump_comments():
    print("Streaming Reddit comments mentioning 'Trump'...")

    for comment in reddit.subreddit('all').stream.comments(skip_existing=True):
        if 'trump' in comment.body.lower():
            data = {
                'id': comment.id,
                'body': comment.body,
                'author': str(comment.author),
                'created_utc': comment.created_utc,
                'score': comment.score,
                'subreddit': comment.subreddit.display_name,
                'permalink': comment.permalink
            }
            producer.send(kafka_topic, value=data)
            print(f"Sent comment: {comment.body[:100]}")

            # Optional sleep to control stream rate
            time.sleep(1)

if __name__ == '__main__':
    stream_trump_comments()