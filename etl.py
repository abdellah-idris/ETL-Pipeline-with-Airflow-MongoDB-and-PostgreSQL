from extract import extract_impl
from transform import transform_impl


news = []

tweets = extract_impl.extract_data('EmmanuelMacron', 1)
tweet = transform_impl.tweet_transfomation('EmmanuelMacron', tweets)

news.append(tweet)

for new in news:
    print(new)