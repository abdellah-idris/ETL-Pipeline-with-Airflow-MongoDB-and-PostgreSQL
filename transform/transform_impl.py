from connexion import connect

api = connect.api_connect()


def tweet_transfomation(user_name, tweets):
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

    for tweet in tweets:
        news[user_name]['TWEET_INFO']['text'].append(tweet._json['full_text'])
    # user = api.get_user(screen_name=user_name)
    # news[user_name]["USER_INFO"]['description'] = user.description

    return news
