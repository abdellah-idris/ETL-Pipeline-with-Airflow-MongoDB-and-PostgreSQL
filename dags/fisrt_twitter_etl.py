from datetime import timedelta
import pymongo
import tweepy
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable



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


def extract_impl(user_name, tweets_count, ti):
    # get user tweets
    tweets = api.user_timeline(screen_name='@{}'.format(user_name),
                               count=tweets_count,
                               include_rts=False,
                               tweet_mode='extended')

    ti.xcom_push(key='tweets', value=tweets)
    ti.xcom_push(key='user_name', value=user_name)



def transform_impl(ti):
    tweets = ti.xcom_pull(task_ids='extract', key='tweets')
    user_name = ti.xcom_pull(task_ids='extract', key='user_name')
    data = {
        user_name: {
            'TWEET_INFO': {
                'text': []
            },
            # "USER_INFO": {
            #     "description": ''
            # }
        }
    }

    for tweet in tweets:
        data[user_name]['TWEET_INFO']['text'].append(tweet._json['full_text'])
    # user = api.get_user(screen_name=user_name)
    # news[user_name]["USER_INFO"]['description'] = user.description

    ti.xcom_push(key='data', value=data)


def load_impl(ti):
    data = ti.xcom_pull(task_ids='transform', key='data')
    client = mongo_connect()
    # Select the database and collection
    print(client.list_database_names())

    db = client['etl']
    collection = db['NEWS']
    # Insert the data into the collection
    if data:
        collection.insert_many(data)
        print('Data loaded successfully')
    else:
        print('Empty data')
    client.close()

default_args = {
    'owner': 'idris',
    'retries': 5,
    'retry_delay': timedelta(minutes=10)
}

with DAG(
    dag_id="my_first_etlv1",
    default_args=default_args,
    description="Fist ETl - simple",
    start_date=pendulum.datetime(2023, 1 ,28,21),
    schedule_interval='@hourly',

) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_impl,
        op_kwargs={'user_name': 'elonmusk', 'tweets_count': 1}
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_impl,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_impl
    )

    extract >> transform >> load