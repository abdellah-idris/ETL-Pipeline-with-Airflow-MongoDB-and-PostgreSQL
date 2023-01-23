from extract import extract_impl
from transform import transform_impl
import argparse

parser = argparse.ArgumentParser(description='Twitter accounts')
parser.add_argument('--accounts', nargs='+', default=[],help='twitter accounts for retrieving tweets, must be a String')


args = parser.parse_args()
news = {}

if __name__ == '__main__':
    for account in args.accounts:
        tweets = extract_impl.extract_data(account, 1)
        tweet = transform_impl.tweet_transfomation(account, tweets)

        if account in news:
            news[account].append([tweet[account]])
        else:
            news[account] = [tweet[account]]
    for key, values in news.items():
        print(key)
        for tweet in values:
            print(tweet)

