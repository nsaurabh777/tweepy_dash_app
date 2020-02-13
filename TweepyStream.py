# import dependencies
import tweepy
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
from unidecode import unidecode
import time
import datetime
# import the API keys from the config file.
from config import con_key, con_sec, a_token, a_secret
import sqlite3


conn = sqlite3.connect("database/db.sqlite")
c = conn.cursor()


def create_table():
    c.execute(
            """
            CREATE TABLE IF NOT EXISTS wineTweets(TIMESTAMP REAL, tweet TEXT)
            """
        )
    conn.commit()
create_table()


class WineListener(StreamListener):
    def on_data(self, data):
        try:
            data = json.loads(data)
            print(data)
            tweet = unidecode(data['text'])
            ts = data['timestamp_ms']           
            #print the stream data to debug
            #print(tweet, time_ms)
            c.execute(
                    """
                    INSERT INTO wineTweets (timestamp, tweet) VALUES (?, ?)
                    """
                , (ts, tweet))
            conn.commit()
            
            #slow the stream
            time.sleep(2)
        except KeyError as e:
            print(str(e))
        return(True)
        
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False


while True:
    try:
        auth = OAuthHandler(con_key, con_sec)
        auth.set_access_token(a_token, a_secret)
        twitterStream = tweepy.Stream(auth, WineListener())
        twitterStream.filter(track=['wine'])
    except Exception as e:
        print(str(e))
        time.sleep(4)
