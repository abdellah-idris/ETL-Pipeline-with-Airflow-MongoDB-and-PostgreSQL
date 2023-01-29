from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
import pymongo
import tweepy


# Access variables
access_key = Variable.get('ACCESS_TOKEN')
access_secret = Variable.get('ACCESS_TOKEN_SECRET')
consumer_key = Variable.get('CONSUMER_KEY')
consumer_secret = Variable.get('CONSUMER_SECRET')

# Twitter authentication
auth = tweepy.OAuthHandler(access_key, access_secret)
auth.set_access_token(consumer_key, consumer_secret)

mongo_password = Variable.get('MONGO')
URI = 'mongodb+srv://dola:{}@mycluster.hlqcjlo.mongodb.net/?retryWrites=true&w=majority'.format(mongo_password)


def api_connect():
    # Creating an API object
    return tweepy.API(auth)


def mongo_connect():
    print('Creating a MongoDB connection')
    try:
        client = pymongo.MongoClient(URI, serverSelectionTimeoutMS=10000)
        return client
    except Exception:
        print("Unable to connect to mongo server.")
        raise Exception


api = api_connect()

default_args = {
    'owner': 'idris',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='twitter_api_v0',
     default_args=default_args,
     start_date=datetime(2023, 1, 29),
     schedule_interval=timedelta(minutes=30))
def twitter_etl():

    @task(multiple_outputs=True)
    def extract_transform(users, tweets_count):
        print('<<< Extracting data from Twitter API')
        data = {}
        for user_name in users:

            print('Extracting tweets from user: {}'.format(user_name))
            # get user tweets
            tweets = api.user_timeline(screen_name='@{}'.format(user_name),
                                       count=tweets_count,
                                       include_rts=False,
                                       tweet_mode='extended'
                                       )
            # TODO : Transform the data
            for tweet in tweets:
                print('Extracted tweet :', tweet)
                if  user_name not in data.keys():
                    data[user_name] = tweet._json['full_text']
                else:
                    data[user_name] = data[user_name] + ';;' + tweet._json['full_text']
        print('Exported data :', data)
        print('>>> Data extracted successfully')
        return {
            'data': data,
        }


    @task()
    def clear():
        print('<<< Clear data from MongoDB')
        client = mongo_connect()
        # Select the database and collection
        print(client.list_database_names())

        db = client['etl']
        collection = db['NEWS']

        collection.delete_many({})
        print('>>> Data cleared successfully')

    @task()
    def load(data):
        print('<<< Loading data into MongoDB')
        tweets = []
        print('Loading data into MongoDB')
        client = mongo_connect()
        # Select the database and collection
        print(client.list_database_names())

        db = client['etl']
        collection = db['NEWS']
        # Insert the data into the collection
        for user_name in data.keys():
            print('Data to load :', data[user_name])
            user_tweet = {
                user_name: {
                    'TWEET_INFO': {
                        'text': []
                    },
                    # "USER_INFO": {
                    #     "description": ''
                    # }
                }
            }
            extracted_tweet = data[user_name].split(';;')
            for tweet in extracted_tweet:
                user_tweet[user_name]['TWEET_INFO']['text'].append(tweet)
            tweets.append(user_tweet)
        if tweets != '' and tweets is not None:
            collection.insert_many(tweets)
            print('>>> Data loaded successfully')
        else:
            print('Empty data')
        client.close()
    twitter_accounts= Variable.get('Tweeter_Accounts').split(',')
    extract_dict = extract_transform(twitter_accounts, 2)
    clear()
    load(extract_dict['data'])


etl_dag = twitter_etl()