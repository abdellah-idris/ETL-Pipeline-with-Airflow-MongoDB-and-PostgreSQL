from tweeter_api import connect

api = connect.api_connect()


def extract_data(user_name, tweets_count):
    # get user tweets
    tweets = api.user_timeline(screen_name='@{}'.format('EmmanuelMacron'),
                               count=tweets_count,
                               include_rts=False,
                               tweet_mode='extended')
    return tweets
