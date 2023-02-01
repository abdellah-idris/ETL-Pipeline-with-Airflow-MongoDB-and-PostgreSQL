from airflow.decorators import dag, task
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import tweepy

# Access variables
access_key = Variable.get('ACCESS_TOKEN')
access_secret = Variable.get('ACCESS_TOKEN_SECRET')
consumer_key = Variable.get('CONSUMER_KEY')
consumer_secret = Variable.get('CONSUMER_SECRET')

# Twitter authentication
auth = tweepy.OAuthHandler(access_key, access_secret)
auth.set_access_token(consumer_key, consumer_secret)
tweet_count = int(Variable.get('Tweets_count'))

def api_connect():
    # Creating an API object
    return tweepy.API(auth)

api = api_connect()

default_args = {
    'owner': 'idris',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(
    dag_id='dag_with_postgres_operator_v0',
    default_args=default_args,
    start_date=datetime(2023, 1, 31),
    schedule_interval='0 * * * *'
)
def twitter_postgres_dag():

    def create_postgres_table():
        print('<<< Creating a PostgreSQL table')
        pg = PostgresOperator(
            task_id='create_postgres_table',
            postgres_conn_id='postgres_localhost',
            sql="""
            CREATE TABLE IF NOT EXISTS  tweets (
                id SERIAL NOT NULL PRIMARY KEY,
                tweet json NOT NULL
                );
            """,
        )
        pg.execute(context=None)

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
            # TODO : Transform the data
            for tweet in tweets:
                print('Extracted tweet : {}'.format(tweet))

                if user_name not in data.keys():
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

        print('Exported data : {}'.format(data))
        print('>>> Data extracted successfully')

        return {
            'data': data,
            'date': date,
            'followers_count': followers_count,
            'following_count': following_count,
            'created_at': created_at,
            'description': description
        }


    @task()
    def transform(data, date, followers_count, following_count, created_at, description):
        import re
        print('<<< Loading data into PostgreSQL')
        tweets = []

        # Insert the data into the collection
        for user_name in data.keys():
            user_tweet = {
                user_name: {
                    "TWEET_INFO": {
                        "text": [],
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

                print('tweet before: {}'.format(tweet))
                tweet = re.sub(r"'", r'', tweet)
                print('tweet after: {}'.format(tweet))
                user_tweet[user_name]['TWEET_INFO']['text'].append(tweet)


            for created_at_date in extracted_date:
                user_tweet[user_name]["TWEET_INFO"]['created_at'].append(created_at_date)

            user_tweet[user_name]["USER_INFO"]['followers_count'] = extracted_followers_count
            user_tweet[user_name]["USER_INFO"]['following_count'] = extracted_following_count
            user_tweet[user_name]["USER_INFO"]['created_at'] = extracted_created_at
            user_tweet[user_name]["USER_INFO"]['description'] = extracted_description.replace("'",'')

            tweets.append(user_tweet)

        if tweets is not None:
            print('<<< Data loaded {}'.format(tweets))
            print('>>> send request')
            return tweets
        else:
            print('>>> [Warning] Empty data : no loaded data ')

    @task()
    def insert_data(data):
        state = False
        hook = PostgresHook(postgres_conn_id ='postgres_localhost')

        for tweet_data in data :
            try:
                print('insert data {}'.format(tweet_data))
                 # TODO fix
                import json
                query = 'INSERT INTO tweets (tweet) VALUES (\'{}\');'.format(json.dumps(tweet_data))
                hook.run(query)
                print('<<<<<<<< Data inserted {}'.format(tweet_data))
                state = True
            except Exception as e:
                print('[Error] : {}'.format(e))
        print('>>> Data inserted ') if state  else print('[ERROR] Failed to insert data')

    # insert = PostgresOperator(
    #     task_id="insert",
    #     postgres_conn_id="postgres_localhost",
    #     sql="sql/insert.sql",
    # )


    twitter_accounts = Variable.get('Tweeter_Accounts').split(',')
    create_postgres_table()
    extracted_data = extract_transform(twitter_accounts, tweet_count)
    data = transform(extracted_data['data'], extracted_data['date'], extracted_data['followers_count'], extracted_data['following_count'], extracted_data['created_at'], extracted_data['description'])
    insert_data(data)

twitter_postgres_dag = twitter_postgres_dag()