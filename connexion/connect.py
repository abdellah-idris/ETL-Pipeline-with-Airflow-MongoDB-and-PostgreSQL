import os
import pymongo
import tweepy
from dotenv import load_dotenv

# Load variables from .env file
load_dotenv()

# Access variables
access_key = os.getenv('ACCESS_TOKEN')
access_secret = os.getenv('ACCESS_TOKEN_SECRET')
consumer_key = os.getenv('CONSUMER_KEY')
consumer_secret = os.getenv('CONSUMER_SECRET')

# Twitter authentication
auth = tweepy.OAuthHandler(access_key, access_secret)
auth.set_access_token(consumer_key, consumer_secret)

URI = 'mongodb+srv://dola:{}@mycluster.hlqcjlo.mongodb.net/?retryWrites=true&w=majority'.format(os.getenv('MONGODB_PASSWORD'))


def api_connect():
    # Creating an API object
    return tweepy.API(auth)


def mongo_connect():
    # Creating a MongoDB connection
    client = pymongo.MongoClient(URI, serverSelectionTimeoutMS=5000)
    try:
        print(client.server_info())
        return client
    except Exception:
        print("Unable to connect to the server.")

