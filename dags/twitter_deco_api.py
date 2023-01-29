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


@dag(dag_id='twitter_api_v14',
     default_args=default_args,
     start_date=datetime(2023, 1, 29),
     schedule_interval=timedelta(minutes=30))
def twitter_etl():

    @task(multiple_outputs=True)
    def extract_transform(user_name, tweets_count):
        print('Extracting tweets from user: {}'.format(user_name))
        # get user tweets
        tweets = api.user_timeline(screen_name='@{}'.format(user_name),
                                   count=tweets_count,
                                   include_rts=False,
                                   tweet_mode='extended')
        # TODO : Transform the data
        for tweet in tweets:
            text= tweet._json['full_text']
        if not tweets:
            print('No tweets found')
            return {
                'tweet': '',
                'user_name': user_name
            }
        return {
            'tweet': text,
            'user_name': user_name
        }


    @task()
    def clear():
        print('clear data from MongoDB')
        client = mongo_connect()
        # Select the database and collection
        print(client.list_database_names())

        db = client['etl']
        collection = db['NEWS']

        collection.delete_many({})
        print('Data cleared successfully')

    @task()
    def load(user_name, data):
        print('Loading data into MongoDB')
        client = mongo_connect()
        # Select the database and collection
        print(client.list_database_names())

        db = client['etl']
        collection = db['NEWS']
        # Insert the data into the collection
        news = {
            user_name: {
                'TWEET_INFO': {
                    'text': []
                },
                # "USER_INFO": {
                #     "description": ''
                # }
            }
        }

        news[user_name]['TWEET_INFO']['text'].append(data)
        print(news)
        if data != '' and data is not None:
            collection.insert(news)
            print('Data loaded successfully')
        else:
            print('Empty data')
        client.close()

    extract_dict = extract_transform('Le_Figaro', 1)
    clear()
    load(user_name=extract_dict['user_name'], text=extract_dict['tweet'])


etl_dag = twitter_etl()