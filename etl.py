import argparse
from extract import extract_impl
from transform import transform_impl
from load import load_impl

parser = argparse.ArgumentParser(description='Twitter accounts')
parser.add_argument('--accounts', nargs='+', default=[], help='twitter accounts for retrieving tweets, must be a String')


args = parser.parse_args()
news = {}
data = []
if __name__ == '__main__':
    for account in args.accounts:
        tweets = extract_impl.extract_data(account, 1)
        tweet = transform_impl.tweet_transfomation(account, tweets)
        data.append(tweet)
    print(data)
    load_impl.load(data)


