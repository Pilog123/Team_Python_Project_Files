import pandas as pd
import numpy as np
import re
import emoji
from threading import *           
import time  
import cx_Oracle
import schedule
from collections import Counter

import tweepy
import configparser

import nltk   
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')


def reply_analysis():
    config = configparser.ConfigParser()
    config.read('config.ini')

    bearer_token = config['twitter']['BEARER_TOKEN']    
    api_key = config['twitter']['API_KEY']
    api_secret = config['twitter']['API_KEY_SECRET']
    access_key = config['twitter']['ACCESS_TOKEN']
    access_secret = config['twitter']['ACCESS_TOKEN_SECRET']


    # Twitter authentication
    auth = tweepy.OAuthHandler(api_key, api_secret)   
    auth.set_access_token(access_key, access_secret) 

    # Creating an API object 
    api = tweepy.API(auth,wait_on_rate_limit=True)
    
    client = tweepy.Client(bearer_token=bearer_token, consumer_key=api_key, consumer_secret=api_secret, access_token=access_key, 
              access_token_secret=access_secret, wait_on_rate_limit=True)

    con1 = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    timeline_df = pd.read_sql(''' select * from TW_BY_LEADERS_TIMELINE''',con1)[15000:]
    reply_df = pd.read_sql(""" select CANDIDATE_NAME,REPLIED_TWEET_ID,SENTIMENT_ANALYSIS_RESULT,
                           TRANSLATED_SENTIMENT_RESULT from TW_BY_PUBLIC_ANALYSIS 
                           where REPLIED_TWEET_ID != 'VALUE NOT AVAILABLE'""",con1)

    timeline_df.fillna('',inplace=True)
    reply_df.fillna('',inplace=True)
    # reply_df = pd.read_excel('ALL_PARTY_PUBLIC_TWEETS.xlsx')


    def t_fun(p,q):
    
        l = []
        for i, j, k in zip(reply_df['REPLIED_TWEET_ID'], reply_df['SENTIMENT_ANALYSIS_RESULT'], reply_df['CANDIDATE_NAME']):
    
            if (i == p) & (k == q):
                l.append(j)
    
        reply_dict = {p : l
        }
    
        return reply_dict[p]
    
    
    def t_fun2(p,q):
        
        l = []
        for i, j, k in zip(reply_df['REPLIED_TWEET_ID'], reply_df['TRANSLATED_SENTIMENT_RESULT'], reply_df['CANDIDATE_NAME']):
    
            if (i == p) & (k == q):
                l.append(j)
    
        reply_dict = {p : l
        }
    
        return reply_dict[p]


    # def like_retweet_update(tweet_id):
    #     try:
    #         status = api.get_status(tweet_id, tweet_mode = "extended")
    #         retweet_count = status.retweet_count 
    #         like_count =  status.favorite_count
    #     except:
    #         retweet_count = 'TWEET DELETED'
    #         like_count = 'TWEET DELETED'
    #     return like_count, retweet_count
    
    def count_update(id):
        try:
            response = client.get_tweets(
                            ids=[id],
                            tweet_fields=["public_metrics"]
                        )
            result_count = response.data[0].public_metrics
            return result_count
        except:
            return 'TWEET DELETED'


    print(timeline_df.shape)
    
    timeline_df['REPLY'] = timeline_df.apply(lambda x:t_fun(x.TWEET_ID, x.CANDIDATE_NAME), axis=1)
    print('REPLY1 collected')
    timeline_df['REPLY'] = timeline_df[timeline_df['REPLY'].notnull()]['REPLY'].apply(lambda x:','.join(x)) 
    timeline_df['REPLY2'] = timeline_df.apply(lambda x:t_fun2(x.TWEET_ID, x.CANDIDATE_NAME), axis=1)
    print('REPLY2 collected')
    timeline_df['REPLY2'] = timeline_df[timeline_df['REPLY2'].notnull()]['REPLY2'].apply(lambda x:','.join(x))

    print('REPLY collected')

    timeline_df = timeline_df[(timeline_df['REPLY'] != '') & (timeline_df['REPLY2'] != '')]
    timeline_df['REPLY'] = timeline_df['REPLY'].apply(lambda x:x.split(','))
    timeline_df['REPLY2'] = timeline_df['REPLY2'].apply(lambda x:x.split(','))
    
    timeline_df['POSITIVE'] = timeline_df['REPLY'].apply(lambda x:x.count('POSITIVE'))
    timeline_df['NEGATIVE'] = timeline_df['REPLY'].apply(lambda x:x.count('NEGATIVE'))
    timeline_df['NEUTRAL'] = timeline_df['REPLY'].apply(lambda x:x.count('NEUTRAL'))
    timeline_df['SENTIMENT_RESULT'] = timeline_df['REPLY'].apply(lambda x:max(Counter(x), key=Counter(x).get))
    
    
    timeline_df['TRANSLATED_POSITIVE'] = timeline_df['REPLY2'].apply(lambda x:x.count('POSITIVE'))
    timeline_df['TRANSLATED_NEGATIVE'] = timeline_df['REPLY2'].apply(lambda x:x.count('NEGATIVE'))
    timeline_df['TRANSLATED_NEUTRAL'] = timeline_df['REPLY2'].apply(lambda x:x.count('NEUTRAL'))
    timeline_df['TRANSLATED_SENTIMENT_RESULT'] = timeline_df['REPLY2'].apply(lambda x:max(Counter(x), key=Counter(x).get))
    print('sentiment count collected')
    
    
    timeline_df['updated_count'] = timeline_df['TWEET_ID'].apply(lambda x : count_update(x))
    print('count updated')
    timeline_df['LIKES_COUNT'] = timeline_df[timeline_df['updated_count']!='TWEET DELETED']['updated_count'].apply(lambda x: int(x['like_count']))
    timeline_df['RETWEET_COUNT'] = timeline_df[timeline_df['updated_count']!='TWEET DELETED']['updated_count'].apply(lambda x: int(x['retweet_count']))
    timeline_df['QUOTE_TWEETS_COUNT'] = timeline_df[timeline_df['updated_count']!='TWEET DELETED']['updated_count'].apply(lambda x: int(x['quote_count']))
    timeline_df['TWEET_URL'] = timeline_df['TWEET_ID'].apply(lambda x:'https://twitter.com/twitter/statuses/'+'{}'.format(x))
    print(timeline_df.columns)
    timeline_df = timeline_df[['CANDIDATE_NAME', 'TWEET_ID', 'PUBLISHED_DATE', 'TWEET_TIME', 'USER_ID',
                            'USER_SCREEN_NAME', 'USER_LOCATION', 'USER_FOLLOWERS', 'DEVICE_SOURCE',
                            'TWEET_CONTENT', 'LIKES_COUNT', 'RETWEET_COUNT', 'USER_MENTIONS',
                            'HASTAGS_IN_TWEET', 'URLS_ATTACHED', 'EMOJI_IN_TWEET', 
                            'POSITIVE','NEGATIVE', 'NEUTRAL', 'SENTIMENT_RESULT', 'CONSTITUENCY_NAME',
                            'TAGGED_PERSON_PARTY_NAME','TWEET_URL','TWEET_GEO_LOCATION', 
                            'TWEET_GEO_COORDINATES','QUOTE_TWEETS_COUNT','TRANSLATED_POSITIVE','TRANSLATED_NEGATIVE',
                            'TRANSLATED_NEUTRAL', 'TRANSLATED_SENTIMENT_RESULT']]
    convert_datatype = {'PUBLISHED_DATE':str,'TWEET_TIME':str,'TWEET_ID':str,'USER_ID':str,
                        'USER_FOLLOWERS':str,'LIKES_COUNT':str,'RETWEET_COUNT':str,'NEGATIVE':str,
                        'POSITIVE':str,'NEUTRAL':str,'TWEET_GEO_COORDINATES':str,'QUOTE_TWEETS_COUNT':str,
                        'TRANSLATED_NEGATIVE':str,'TRANSLATED_POSITIVE':str,'TRANSLATED_NEUTRAL':str}

    timeline_df = timeline_df.astype(convert_datatype)
    timeline_df.drop_duplicates('TWEET_ID',inplace=True)
    timeline_df.fillna('',inplace=True)
    timeline_df.reset_index(drop=True,inplace=True)
    # print(timeline_df)

    print(timeline_df.columns)
    # d1 = pd.read_excel('ALL_PARTY_TAGGED_TWEETS.xlsx')
    # final_df = pd.concat([df,d1]) 

    # timeline_df.to_excel('REPLY_ANALYSIS_RESULT.xlsx', index = False, encoding = 'utf-16')

    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    cursor = con.cursor()

    cursor1 = con.cursor()
    for i in range(len(timeline_df)):
        data1 = [[timeline_df['CANDIDATE_NAME'][i],timeline_df['TWEET_ID'][i],timeline_df['PUBLISHED_DATE'][i],timeline_df['TWEET_TIME'][i],
                timeline_df['USER_ID'][i],timeline_df['USER_SCREEN_NAME'][i],timeline_df['USER_LOCATION'][i],
                timeline_df['USER_FOLLOWERS'][i],timeline_df['DEVICE_SOURCE'][i],timeline_df['TWEET_CONTENT'][i],timeline_df['LIKES_COUNT'][i],
                timeline_df['RETWEET_COUNT'][i],timeline_df['USER_MENTIONS'][i],timeline_df['HASTAGS_IN_TWEET'][i],
                timeline_df['URLS_ATTACHED'][i],timeline_df['EMOJI_IN_TWEET'][i],timeline_df['POSITIVE'][i],
                timeline_df['NEGATIVE'][i],timeline_df['NEUTRAL'][i],timeline_df['SENTIMENT_RESULT'][i],timeline_df['CONSTITUENCY_NAME'][i],
                timeline_df['TAGGED_PERSON_PARTY_NAME'][i],'KEERTHI','KEERTHI',timeline_df['TWEET_URL'][i],
                timeline_df['TWEET_GEO_LOCATION'][i], timeline_df['TWEET_GEO_COORDINATES'][i],timeline_df['QUOTE_TWEETS_COUNT'][i],
                timeline_df['TRANSLATED_POSITIVE'][i],timeline_df['TRANSLATED_NEGATIVE'][i],timeline_df['TRANSLATED_NEUTRAL'][i],
                timeline_df['TRANSLATED_SENTIMENT_RESULT'][i]]]
        # print(data1)
        cursor.prepare("""INSERT INTO TW_TIMELINE_REPLY_ANALYSIS(CANDIDATE_NAME,
        TWEET_ID,PUBLISHED_DATE,TWEET_TIME,USER_ID,USER_SCREEN_NAME,USER_LOCATION,
        USER_FOLLOWERS,DEVICE_SOURCE,TWEET_CONTENT,LIKES_COUNT,RETWEET_COUNT,USER_MENTIONS,
        HASTAGS_IN_TWEET,URLS_ATTACHED,EMOJI_IN_TWEET,POSITIVE_SENTIMENT_COUNT,
        NEGATIVE_SENTIMENT_COUNT,NEUTRAL_SENTIMENT_COUNT,SENTIMENT_ANALYSIS_RESULT,
	    CONSTITUENCY_NAME,CANDIDATE_PARTY_NAME,CREATE_BY,EDIT_BY,TWEET_URL,
	    TWEET_GEO_LOCATION, TWEET_GEO_COORDINATES, QUOTE_TWEETS_COUNT, TRANSLATED_POSITIVE_COUNT,
        TRANSLATED_NEGATIVE_COUNT, TRANSLATED_NEUTRAL_COUNT, TRANSLATED_SENTIMENT_RESULT) 
        VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,
                :28,:29,:30,:31)""")
        cursor.executemany(None, data1)
        con.commit()


        cursor1.execute("""DELETE FROM TW_TIMELINE_REPLY_ANALYSIS
                WHERE rowid not in
                (SELECT MIN(rowid)
                FROM TW_TIMELINE_REPLY_ANALYSIS
                GROUP BY TWEET_ID)
                """)
        con.commit()

    cursor.close()
    cursor1.close()
    con.close()
    con1.close()



schedule.every().day.at("10:45").do(reply_analysis)
while True:
    schedule.run_pending()
    time.sleep(1)


