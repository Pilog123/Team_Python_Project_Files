# -*- coding: utf-8 -*-
"""
Created on Tue May 31 14:52:05 2022

@author: Keerthi
"""
##################################### TWEETS TAGGING TRS MEMBERS ##############################################

import pandas as pd
from tqdm.notebook import tqdm_notebook
tqdm_notebook.pandas()
import re
import emoji

import tweepy
import configparser

import nltk   
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

import schedule
import time
import datetime

import cx_Oracle

def twitter_df():
    config = configparser.ConfigParser()
    config.read('config.ini')
    
    api_key = config['twitter']['API_KEY']
    api_secret = config['twitter']['API_KEY_SECRET']
    access_key = config['twitter']['ACCESS_TOKEN']
    access_secret = config['twitter']['ACCESS_TOKEN_SECRET']
    
    
    # Twitter authentication
    auth = tweepy.OAuthHandler(api_key, api_secret)   
    auth.set_access_token(access_key, access_secret) 
    
    # Creating an API object 
    api = tweepy.API(auth,wait_on_rate_limit=True)
    
    
    today = datetime.date.today()
    #day = datetime.date.today()
    #today = day - datetime.timedelta(days=1)
    yesterday= today - datetime.timedelta(days=1)
    
    mem_df = pd.read_excel('Candidate_list.xlsx')
    
    list_details = []
    for cand_name,name,party,constitunecy in zip(mem_df['WINNER CANDIDATE'],mem_df['TWITTER_SCREEN_NAMES'],
                                       mem_df['TAGGED_PERSON_PARTY'], mem_df['CONSTITUENCY']):
        new_tweets = tweepy.Cursor(api.search_tweets, q="@{} -filter:retweets since:".format(name) + str(yesterday)+ " until:" + str(today) ,
                               tweet_mode='extended')
        
    
        for tweet in new_tweets.items():
            tweet_text = []
            if tweet.full_text.startswith("RT @") == True:
                tweet_text.append(tweet.retweeted_status.full_text)
            else:
                tweet_text.append(tweet.full_text)
            text = tweet._json["full_text"]
            if tweet.place is not None:
                place = tweet.place.full_name
                coord = tweet.place.bounding_box.coordinates
                print('\n',tweet.place.full_name,'\n')
                print(tweet.place.bounding_box.coordinates)
            else:
                place = tweet.place
                coord = tweet.place
            print(cand_name,'\n',text)
    
            refined_tweet = {'created_at' : tweet.created_at,
                             'TWEET_ID' : tweet.id_str,
                             'USER_ID' : tweet.user.id_str,
                             'USER_SCREEN_NAME' : tweet.user.screen_name,
                             'USER_LOCATION' : tweet.user.location,
                             'USER_FOLLOWERS' : tweet.user.followers_count,
                             'SOURCE' : tweet.source,
                             'TWEET' : tweet_text[0],
                             'LIKES' : tweet.favorite_count,
                             'RETWEET_COUNT' : tweet.retweet_count,
                             'NAME' : cand_name,
                             'CONSTITUENCY' : constitunecy,
                             'TAGGED_PERSON_PARTY': party,
                             'REPLIED_TWEET_ID' : tweet.in_reply_to_status_id_str,
                             'TWEET_GEO_LOCATION' : place,
                             'TWEET_GEO_COORDINATES' : coord,
                        }
    
            list_details.append(refined_tweet)
    
    
    df = pd.DataFrame(list_details)
    df['created_at'] = df['created_at'].dt.tz_convert('Asia/Kolkata')
    df['DATE'] = pd.to_datetime(df['created_at']).dt.date
    df['TIME'] = pd.to_datetime(df['created_at']).dt.time
   
    df.drop(['created_at'],axis=1,inplace=True)
    print(df)
    
    df.to_excel('public_tweets_1.xlsx',index=False, encoding='utf-16')
    
    
    df['TWEET'].replace(r'\n',' ',inplace = True, regex = True)
    df['TWEET'].replace(r'\.\.+',' ',inplace = True, regex = True)
    df['URLS_ATTACHED'] = df['TWEET'].apply(lambda x:','.join([i.group().strip() for i in re.finditer(r'https?://\S+|www\.\S+',x)]))
    df['HASTAGS_IN_TWEET'] = df['TWEET'].apply(lambda x:','.join([i.group().strip().replace('#','') for i in re.finditer(r'#(.*?)(\s|$)',x)]))
    df['USER_MENTIONS'] = df['TWEET'].apply(lambda x:','.join([i.group().strip().replace('@','') for i in re.finditer(r'@(.*?)(\s|$)',x)]))
    df['EMOJI_IN_TWEET'] = df['TWEET'].apply(lambda x:','.join([i.group().strip() for i in re.finditer(emoji.get_emoji_regexp(), x)]))
    df['TWEET'] = df['TWEET'].apply(lambda x:re.sub(r'https?://\S+|www\.\S+',' ',x))
    df['trimmed_tweet'] = df['TWEET'].apply(lambda x:re.sub(r'(@|#)(.*?)(\s|$)',' ',x))
    df['trimmed_tweet'] = df['trimmed_tweet'].apply(lambda x:re.sub(emoji.get_emoji_regexp(), r"", x))
    df['trimmed_tweet'] = df['trimmed_tweet'].apply(lambda x:re.sub(r'  +',' ',x))
    
    
    # df['trimmed_tweet'] = df['trimmed_tweet'].apply(lambda x:re.sub(r'  +',' ',x))
    df = df[(df['trimmed_tweet'] != '') & (df['trimmed_tweet'] != ' ')]
    # df = df[df['trimmed_tweet'] != ' ']
    df.reset_index(drop=True,inplace=True)
    
    
    
    
    # names_list = []
    # for i in df['user_mentions']:
    #     names_list.append(i[0])
    # names_list
    
    
    # pd.Series(names_list).value_counts().head(15).plot.bar()
    
    ################################################  SENTIMENT ANALYSIS  ########################################################
    lemma = WordNetLemmatizer()
    sw = stopwords.words('english')
    nsw=["not","couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn',
         "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't",
         'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't","don't","dont",'cant',"can't"]
    new_sw = []
    for word in sw:
        if(word not in nsw):
            new_sw.append(word)
    def text_prep(x: str) -> list:
        corp = str(x).lower() 
        corp = re.sub(r'["!@#\$%\^&*\(\)\[\]\{\};:,./<>?\|`~\-=_+0-9]', " ", corp)
        corp = corp.split()
    #     tokens = word_tokenize(corp)
        words = [t for t in corp if t not in new_sw]
        lemmatize = [lemma.lemmatize(w) for w in words]
        return lemmatize
    # preprocess_tag = [text_prep(i) for i in df['trimmed_tweet']]
    # df["preprocess_txt"] = preprocess_tag
    df['preprocess_txt'] = df['trimmed_tweet'].apply(lambda x:text_prep(x))
    df['TWEET_TOTAL_LENGTH'] = df['preprocess_txt'].map(lambda x: len(x))
    file1 = pd.read_excel('Sentiment_Words.xlsx')
    neg_words = list(file1['Negative_Words'])
    # file2 = open('./opinion-lexicon-English/positive-words.txt', 'r')
    # pos_words = file2.read().split()
    df['combined_tweet'] = [i+list(j) for i,j in zip(df['preprocess_txt'],df['EMOJI_IN_TWEET'])]
    pos_words = list(file1['Positive_Words'])
    num_pos = df['combined_tweet'].map(lambda x: len([i for i in x if i in pos_words]))
    pos_wrd = df['combined_tweet'].map(lambda x: [i for i in x if i in pos_words])
    df['POSITIVE'] = num_pos
    num_neg = df['combined_tweet'].map(lambda x: len([i for i in x if i in neg_words]))
    neg_wrd = df['combined_tweet'].map(lambda x: [i for i in x if i in neg_words])
    df['NEGATIVE'] = num_neg
    df['NEUTRAL'] = df['TWEET_TOTAL_LENGTH'] - (df['POSITIVE'] + df['NEGATIVE'])
    df['SENTIMENT_ANALYSIS'] = round((df['POSITIVE'] - df['NEGATIVE']) / df['TWEET_TOTAL_LENGTH'], 2)
    sent = []
    for i in df['SENTIMENT_ANALYSIS']:
        if i > 0:
            sent.append('POSITIVE')
        elif i < 0:
            sent.append('NEGATIVE')
        else:
            sent.append('NEUTRAL')
    df['SENTIMENT_ANALYSIS'] = sent
    df['TWEET_URL'] = df['TWEET_ID'].apply(lambda x:'https://twitter.com/twitter/statuses/'+'{}'.format(x))
    df = df[['TWEET_ID','DATE', 'TIME', 'USER_ID', 'USER_SCREEN_NAME', 'USER_LOCATION',
        'USER_FOLLOWERS', 'SOURCE', 'TWEET', 'LIKES', 'RETWEET_COUNT', 'USER_MENTIONS', 
        'HASTAGS_IN_TWEET', 'URLS_ATTACHED', 'EMOJI_IN_TWEET', 'TWEET_TOTAL_LENGTH', 
        'POSITIVE', 'NEGATIVE', 'NEUTRAL', 'SENTIMENT_ANALYSIS', 'NAME','CONSTITUENCY', 
        'TAGGED_PERSON_PARTY','REPLIED_TWEET_ID','TWEET_URL','TWEET_GEO_LOCATION', 'TWEET_GEO_COORDINATES']]
    print(df.dtypes)
    df.fillna('VALUE NOT AVAILABLE',inplace=True)
    convert_datatype = {'DATE':str,'TIME':str,'TWEET_ID':str,'USER_ID':str,
                        'USER_FOLLOWERS':str,'LIKES':str,'RETWEET_COUNT':str,'NEGATIVE':str,
                        'TWEET_TOTAL_LENGTH':str,'POSITIVE':str,'NEUTRAL':str,
                        'REPLIED_TWEET_ID':str,'TWEET_GEO_COORDINATES':str}
    
    df = df.astype(convert_datatype)
    
    print(df)
    print(df.dtypes)
    # d1 = pd.read_excel('ALL_PARTY_TAGGED_TWEETS.xlsx')
    # final_df = pd.concat([df,d1]) 
    # df.to_excel('ALL_PARTY_PUBLIC_TWEETS.xlsx',index=False, encoding='utf-16')
    
    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    cursor = con.cursor()
    for i in range(len(df)):
        data1 = [[df['NAME'][i],df['TWEET_ID'][i],df['DATE'][i],df['TIME'][i],
                  df['USER_ID'][i],df['USER_SCREEN_NAME'][i],df['USER_LOCATION'][i],
                  df['USER_FOLLOWERS'][i],df['SOURCE'][i],df['TWEET'][i],df['LIKES'][i],
                  df['RETWEET_COUNT'][i],df['USER_MENTIONS'][i],df['HASTAGS_IN_TWEET'][i],
                  df['URLS_ATTACHED'][i],df['EMOJI_IN_TWEET'][i],df['TWEET_TOTAL_LENGTH'][i],
                  df['POSITIVE'][i],df['NEGATIVE'][i],df['NEUTRAL'][i],df['SENTIMENT_ANALYSIS'][i],
                  df['CONSTITUENCY'][i],df['TAGGED_PERSON_PARTY'][i],df['REPLIED_TWEET_ID'][i],
                  'KEERTHI','KEERTHI',df['TWEET_URL'][i],df['TWEET_GEO_LOCATION'][i], df['TWEET_GEO_COORDINATES'][i]]]
        # print(data1)
        cursor.prepare("""INSERT INTO TW_BY_PUBLIC_ANALYSIS(CANDIDATE_NAME,
        TWEET_ID,PUBLISHED_DATE,TWEET_TIME,USER_ID,USER_SCREEN_NAME,USER_LOCATION,
        USER_FOLLOWERS,DEVICE_SOURCE,TWEET_CONTENT,LIKES_COUNT,RETWEET_COUNT,USER_MENTIONS,
        HASTAGS_IN_TWEET,URLS_ATTACHED,EMOJI_IN_TWEET,TWEET_WORD_COUNT,
        POSITIVE_SENTIMENT_COUNT,NEGATIVE_SENTIMENT_COUNT,NEUTRAL_SENTIMENT_COUNT, 
        SENTIMENT_ANALYSIS_RESULT,CONSTITUENCY_NAME,TAGGED_PERSON_PARTY_NAME,REPLIED_TWEET_ID,
        CREATE_BY,EDIT_BY,TWEET_URL,TWEET_GEO_LOCATION, TWEET_GEO_COORDINATES) VALUES (:0,:1,:2,:3,:4,:5,
        :6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28)""")
        cursor.executemany(None, data1)
        con.commit()
    cursor.close()
    con.close()
        


schedule.every().day.at("07:00").do(twitter_df)
while True:
    schedule.run_pending()
    time.sleep(1)


# df.drop(['created_at'],axis=1,inplace=True)
