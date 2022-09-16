import tweepy
import configparser
import pandas as pd

from datetime import datetime
from pytz import timezone
from fastapi import FastAPI

import uvicorn

app = FastAPI()

def stream_tweets():
    config = configparser.ConfigParser()
    config.read('config.ini')

    bearer_token = config['twitter']['BEARER_TOKEN']
    api_key = config['twitter']['API_KEY']
    api_secret = config['twitter']['API_KEY_SECRET']
    access_key = config['twitter']['ACCESS_TOKEN']
    access_secret = config['twitter']['ACCESS_TOKEN_SECRET']

    client = tweepy.Client(bearer_token, api_key, api_secret, access_key, access_secret)
    auth = tweepy.OAuth1UserHandler(api_key, api_secret, access_key, access_secret)
    api = tweepy.API(auth)

    mem_df = pd.read_excel('Candidate_list.xlsx',sheet_name='Sheet2')


    class Streamer(tweepy.StreamingClient):
        tweets = []
        limit = 20
        def on_tweet(self, tweet):
            tweet_dict = {
                    'TWEET_DATE' : tweet.created_at.astimezone(timezone('Asia/Kolkata')).date(),
                    'TWEET_TIME' : tweet.created_at.astimezone(timezone('Asia/Kolkata')).time(),
                    'USER_ID' : tweet.author_id,
                    'LIKES_COUNT' : tweet.public_metrics['like_count'],
                    'RETWEET_COUNT' : tweet.public_metrics['retweet_count'],
                    'REPLY_COUNT' : tweet.public_metrics['reply_count'],
                    'QUOTE_TWEETS_COUNT' : tweet.public_metrics['quote_count'],
                    'DEVICE_SOURCE' : tweet.source,
                    'TWEET_CONTENT' : tweet.text,
                    'TWEET_URL' : 'https://twitter.com/twitter/statuses/'+'{}'.format(tweet.id)
                }
            self.tweets.append(tweet_dict)
            if len(self.tweets) == self.limit:
                self.disconnect()
            # print(tweet_dict)
    #         return tweet_dict
        def on_error(self, status_code):
            if status_code == 420:
                #returning False in on_data disconnects the stream
                return False

    streamer = Streamer(bearer_token, wait_on_rate_limit = True)

    # search_terms = ['@KTRTRS','@trspartyonline','@bandisanjay_bjp','@TelanganaCMO']

    for name in mem_df['TWITTER_SCREEN_NAMES']:
    # for name in search_terms:
        streamer.add_rules(tweepy.StreamRule('@{}'.format(name)))
    streamer.filter(tweet_fields=['created_at','source','public_metrics','author_id'])

    data = []

    for tweet in streamer.tweets:
        data.append(tweet)
    df = pd.DataFrame(data)
    df['USER_NAME'] = df['USER_ID'].apply(lambda id:api.get_user(user_id = '{}'.format(id)).name)
    convert_datatype = {'TWEET_DATE':str,'TWEET_TIME':str,'LIKES_COUNT':str,'REPLY_COUNT':str,'QUOTE_TWEETS_COUNT':str,
                        'RETWEET_COUNT':str,'USER_ID':str}
    
    df = df.astype(convert_datatype)
    print(df)
    # streamer.filter(tweet_fields = ['referenced_tweets'])
    return df

@app.post("/streaming_tweets/")
# async def streaming_tweets():

    # def allocation(tableName, colsArray, accessName):
    #     con = create_engine('oracle+cx_oracle://{}'.format(accessName))       
    #     print('SELECT {} FROM {}'.format(colsArray, tableName))
    #     df = pd.read_sql('SELECT {} FROM {}'.format(colsArray, tableName) ,con)[:100]
        
    #     return df
            
def connection():
        return stream_tweets() 
    
    #print(DF)
    # return {
    #     stream_tweets()
    # }
@app.get("/")
async def main():
#     content = """
# <body>
# <form action="/uploadfiles/" enctype="multipart/form-data" method="post">
# <input name="file" type="file" multiple>
# <input type="submit">
# </form>
# </body>
#     """
    return #HTMLResponse(content=content)

if __name__ == '__main__':
    uvicorn.run("STREAM_TWEETS_API:app", host = '172.16.1.62',port= 6657, log_level = 'info')
