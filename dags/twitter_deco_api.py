from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable



def api_connect():
    import tweepy
    # Creating an API object
    # Access variables
    access_key = Variable.get('ACCESS_TOKEN')
    access_secret = Variable.get('ACCESS_TOKEN_SECRET')
    consumer_key = Variable.get('CONSUMER_KEY')
    consumer_secret = Variable.get('CONSUMER_SECRET')

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)
    return tweepy.API(auth)


def mongo_connect():
    import pymongo
    print('>>> Creating a MongoDB connection')

    try:
        mongo_password = Variable.get('MONGO')
        URI = 'mongodb+srv://dola:{}@mycluster.hlqcjlo.mongodb.net/?retryWrites=true&w=majority'.format(mongo_password)
        client = pymongo.MongoClient(URI, serverSelectionTimeoutMS=10000)
        return client
    except Exception:
        print("[ERROR] : Unable to connect to mongo server.")
        raise Exception


api = api_connect()

default_args = {
    'owner': 'idris',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}


@dag(dag_id='twitter_dag_with_mongoDB',
     default_args=default_args,
     start_date=datetime(2023, 2, 2),
     schedule_interval='0 * * * *')
def twitter_etl():

    @task(multiple_outputs=True)
    def extract_transform(users, tweets_count):
        print('<<< Extracting data from Twitter API')

        data = {}
        date = {}
        followers_count = {}
        following_count = {}
        created_at = {}
        description = {}

        for user_name in users:
            print('Extracting tweets from user: {}'.format(user_name))
            # get user tweets
            tweets = api.user_timeline(screen_name='@{}'.format(user_name),
                                       count=tweets_count,
                                       include_rts=False,
                                       tweet_mode='extended'
                                       )

            for tweet in tweets:
                print('Extracted tweet : {}'.format( tweet))

                if  user_name not in data.keys():
                    data[user_name] = tweet._json['full_text']
                    date[user_name] = tweet.created_at.strftime('%m/%d/%Y:%H:%M')
                else:
                    data[user_name] = data[user_name] + ';;' + tweet._json['full_text']
                    date[user_name] = date[user_name] + ';;' + tweet.created_at.strftime('%m/%d/%Y:%H:%M')

            user = api.get_user(screen_name=user_name)
            followers_count[user_name] = user.followers_count
            following_count[user_name] = user.friends_count
            created_at[user_name] = user.created_at.strftime('%m/%d/%Y:%H:%M')
            description[user_name] = user.description

        print('Extracted data : {}'.format(data) )
        print('>>> Data extracted successfully')

        return {
            'data': data,
            'date' : date,
            'followers_count' : followers_count,
            'following_count' : following_count,
            'created_at' : created_at,
            'description' : description
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
        print('>>> Data cleared successfully on {}'.format(collection))

    @task()
    def load(data, date, followers_count, following_count, created_at, description):
        print('<<< Loading data into MongoDB')
        tweets = []

        client = mongo_connect()
        # Select the database and collection
        db = client['etl']
        collection = db['NEWS']

        # Insert the data into the collection
        for user_name in data.keys():
            user_tweet = {
                user_name: {
                    'TWEET_INFO': {
                        'text': [],
                        "created_at": []
                    },
                    "USER_INFO": {
                        "followers_count": '',
                        "following_count": '',
                        "created_at": '',
                        "description": ''
                    }
                }
            }

            extracted_tweet = data[user_name].split(';;')
            extracted_date = date[user_name].split(';;')
            extracted_followers_count = followers_count[user_name]
            extracted_following_count = following_count[user_name]
            extracted_created_at = created_at[user_name]
            extracted_description = description[user_name]

            for tweet in extracted_tweet:
                user_tweet[user_name]['TWEET_INFO']['text'].append(tweet)

            for created_at_date in extracted_date:
                user_tweet[user_name]["TWEET_INFO"]['created_at'].append(created_at_date)

            user_tweet[user_name]["USER_INFO"]['followers_count'] = extracted_followers_count
            user_tweet[user_name]["USER_INFO"]['following_count'] = extracted_following_count
            user_tweet[user_name]["USER_INFO"]['created_at'] = extracted_created_at
            user_tweet[user_name]["USER_INFO"]['description'] = extracted_description

            tweets.append(user_tweet)

        if tweets is not None:
            collection.insert_many(tweets)
            print('<<< Data loaded {}'.format(tweets))
            print('>>> Data loaded successfully')
        else:
            print('>>> [Warning] Empty data : no loaded data ')
        client.close()


    tweet_count = int(Variable.get('Tweets_count'))
    twitter_accounts= Variable.get('Tweeter_Accounts').split(',')

    extract_dict = extract_transform(twitter_accounts, tweet_count)
    clear()
    load(extract_dict['data'], extract_dict['date'], extract_dict['followers_count'], extract_dict['following_count'], extract_dict['created_at'], extract_dict['description'])


etl_dag = twitter_etl()