import tweepy
import json
import time
from kafka import KafkaProducer
from configs import consumer_key, consumer_secret, access_token, access_token_secret


### keep the api key info in secret.
consumer_key=consumer_key
consumer_secret=consumer_secret
access_token=access_token
access_token_secret=access_token_secret

### 2. connecting streaming korean tweets to kafka producer (see the docker-compose yaml file to check kafka setting)
producer = KafkaProducer(bootstrap_servers=["localhost:9092"])

class ProcessStream(tweepy.Stream):
  def on_data(self, raw_data):
    data = json.loads(raw_data)
    if "lang" in data and data["lang"] == "ko":
      korean_tweet = {
        "text": data["text"],
        "timestamp_ms": data["timestamp_ms"]
      }
      producer.send("korean-tweets", json.dumps(korean_tweet).encode("utf-8"))

twitter_stream = ProcessStream(
  consumer_key,
  consumer_secret,
  access_token,
  access_token_secret
)

twitter_stream.filter(track=["Twitter"])

# twitter_stream.filter(track=["Twitter"])

# ### 1. let check twitter api works right and how stream data looks like and formatted
# class ProcessStream(tweepy.Stream):
#     def on_data(self, raw_data):
#         data = json.loads(raw_data)
#         if "lang" in data and data["lang"]=="ko":
#             korean_tweet = {
#                 "text": data["text"],
#                 "created_at": data["created_at"],
#                 "timestamp_ms": data["timestamp_ms"],
#             }
#             print(korean_tweet["text"])
#             time.sleep(1)

# twitter_stream = ProcessStream(
#     consumer_key,
#     consumer_secret,
#     access_token,
#     access_token_secret,
# )

# twitter_stream.filter(track=["Twitter"])
