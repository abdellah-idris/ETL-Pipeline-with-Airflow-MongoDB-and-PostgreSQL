from extract import extract_impl
from transform import transform_impl


news = {}

if __name__ == '__main__' :
    tweets = extract_impl.extract_data('EmmanuelMacron', 1)
    tweet = transform_impl.tweet_transfomation('EmmanuelMacron', tweets)

    for new in news:
        print(new)