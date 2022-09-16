# -*- coding: utf-8 -*-
"""
Created on Tue May 31 14:52:05 2022

@author: Keerthi
"""
##################################### LEADERS TIMELINE TWEETS  ##############################################

import pandas as pd
from tqdm.notebook import tqdm_notebook
tqdm_notebook.pandas()
import re
import emoji

import tweepy
import configparser

from googletrans import Translator
trans = Translator()
from langdetect import detect

import nltk   
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import sent_tokenize
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('movie_reviews')
nltk.download('vader_lexicon')

from textblob import TextBlob
from nltk.sentiment import SentimentIntensityAnalyzer
sia = SentimentIntensityAnalyzer()

import schedule
import time
import datetime
import cx_Oracle

def twitter_df():
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
	
    client = tweepy.Client(bearer_token = bearer_token, consumer_key = api_key, consumer_secret = api_secret, 
			access_token = access_key, access_token_secret = access_secret, wait_on_rate_limit = True)


    today = datetime.date.today()
    yesterday= today - datetime.timedelta(days=1)
    
    mem_df = pd.read_excel('Candidate_list.xlsx')
    
    list_details = []
    for cand_name,name,party,constitunecy in zip(mem_df['WINNER CANDIDATE'],mem_df['TWITTER_SCREEN_NAMES'],
                                       mem_df['TAGGED_PERSON_PARTY'], mem_df['CONSTITUENCY']):
        new_tweets = tweepy.Cursor(api.user_timeline, screen_name="@{}".format(name),
                           tweet_mode='extended')
        
        try:
            for tweet in new_tweets.items():
                if tweet.created_at.date() >= yesterday:
                    retweet_text = ''
                    tweet_text = ''
                    if tweet.full_text.startswith("RT @") == True:
                        retweet_text = tweet.retweeted_status.full_text
                        retweet_count = tweet.retweeted_status.retweet_count
                        likes_count = tweet.retweeted_status.favorite_count
                    else:
                        tweet_text = tweet.full_text
                        retweet_count = tweet.retweet_count
                        likes_count = tweet.favorite_count
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
                                     'ACTUAL_TWEET' : tweet_text,
                                     'RETWEET' : retweet_text,
                                     'LIKES' : likes_count,
                                     'RETWEET_COUNT' : retweet_count,
                                     'NAME' : cand_name,
                                     'CONSTITUENCY' : constitunecy,
                                     'TAGGED_PERSON_PARTY': party,
                                     'TWEET_GEO_LOCATION' : place,
                                     'TWEET_GEO_COORDINATES' : coord,
                                }
            
                    list_details.append(refined_tweet)
                else:
                    break
        except:
            pass
        
    df = pd.DataFrame(list_details)
    df['created_at'] = df['created_at'].dt.tz_convert('Asia/Kolkata')
    
    ret_lst = []
    for i,j in zip(df['ACTUAL_TWEET'], df['RETWEET']):
        if i:
            ret_lst.append('NOT_RETWEET')
        else:
            ret_lst.append('RETWEET')

    df['IS_RETWEET'] = ret_lst
    df['TWEET'] = df['ACTUAL_TWEET'] + df['RETWEET']
    print(df)


    df['TWEET'].replace(r'\n',' ',inplace = True, regex = True)
    df['TWEET'].replace(r'\.\.+',' ',inplace = True, regex = True)
    df['URLS_ATTACHED'] = df['TWEET'].apply(lambda x:','.join([i.group().strip() for i in re.finditer(r'https?://\S+|www\.\S+',x)]))
    df['HASTAGS_IN_TWEET'] = df['TWEET'].apply(lambda x:','.join([i.group().strip().replace('#','') for i in re.finditer(r'#(.*?)(\s|$)',x)]))
    df['USER_MENTIONS'] = df['TWEET'].apply(lambda x:','.join([i.group().strip().replace('@','') for i in re.finditer(r'@(.*?)(\s|$)',x)]))
    df['EMOJI_IN_TWEET'] = df['TWEET'].apply(lambda x:','.join([i.group().strip() for i in re.finditer(emoji.get_emoji_regexp(), x)]))
    df['TWEET'] = df['TWEET'].apply(lambda x:re.sub(r'https?://\S+|www\.\S+',' ',x))
    #df['QUOTED_TWEETS_COUNT'] = df['TWEET_ID'].apply(lambda id:client.get_quote_tweets(id).meta['result_count'])
    #df['QUOTED_TWEETS'] = df['TWEET_ID'].apply(lambda id:' |'.join([client.get_quote_tweets(id).data[i].text for i in range(client.get_quote_tweets(id).meta['result_count'])]))

    df['trimmed_tweet'] = df['TWEET'].apply(lambda x:re.sub(r'(@|#)(.*?)(\s|$)',' ',x))
    df['trimmed_tweet'] = df['trimmed_tweet'].apply(lambda x:re.sub(emoji.get_emoji_regexp(), r"", x))
    df['trimmed_tweet'] = df['trimmed_tweet'].apply(lambda x:re.sub(r'  +',' ',x))


    #df['trimmed_tweet'] = df['trimmed_tweet'].apply(lambda x:re.sub(r'  +',' ',x))
    df = df[(df['trimmed_tweet'] != '') & (df['trimmed_tweet'] != ' ')]
    #df = df[df['trimmed_tweet'] != ' ']
    df.reset_index(drop=True,inplace=True)
    

    df['DATE'] = pd.to_datetime(df['created_at']).dt.date
    df['TIME'] = pd.to_datetime(df['created_at']).dt.time
  # names_list = []
    # for i in df['user_mentions']:
    #     names_list.append(i[0])
    # names_list


    # pd.Series(names_list).value_counts().head(15).plot.bar()
    #########################################  SENTIMENT ANALYSIS METHOD 1  ########################################################
    
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
    df['POSITIVE_WORDS'] = df['combined_tweet'].map(lambda x: ','.join([i for i in x if i in pos_words]))
    df['POSITIVE'] = num_pos
    num_neg = df['combined_tweet'].map(lambda x: len([i for i in x if i in neg_words]))
    df['NEGATIVE_WORDS'] = df['combined_tweet'].map(lambda x: ','.join([i for i in x if i in neg_words]))
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
    
    print('METHOD 1 completed')
        ######################################## SENTIMENT ANALYSIS METHOD 2 #############################################
    
    def lang_translator(tweet):
        trans_lst = []
        try:
            trans_lst.append(trans.translate(tweet, src= trans.detect(tweet).lang, dest= 'en').text)
        except:
            try:
                trans_lst.append(trans.translate(tweet, src= trans.detect(tweet).lang[0], dest= 'en').text)
            except:
                trans_lst.append(tweet)
        print(trans_lst)
        return '|'.join(trans_lst)
    
    df['TRANSLATED_TWEET'] = df['trimmed_tweet'].apply(lambda x:lang_translator(x))
    
    print('\n','TRANSLATION completed','\n')
    
    def cleaning_tweet(tweet):
        # remove hyperlinks, special characters and white spaces using 're'
    #     tweet = re.sub(r'https?://\S+|www\.\S+','',tweet)
        tweet = re.sub(r'@[^a-zA-Z0-9\s]+','',tweet)
    #     tweet = re.sub(r'\#','',tweet)
        tweet = re.sub(r'[^\w]', ' ', tweet)
        # removing white spaces
        tweet = re.sub('\s+',' ',tweet)
        # remove start and end spaces and convert all text to lower case
        tweet = tweet.strip()
        #convertig to lower case
        tweet = tweet.lower()
        
        txt=sent_tokenize(tweet)
        
        new_sw = []
        corpus_lst = []
        
        wc = WordNetLemmatizer()
        sw = stopwords.words('english')
        nsw=["not","couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn',
             "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't",
             'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't"]
    
        for word in sw:
            if(word not in nsw):
                new_sw.append(word)
        for i in txt:
            if i not in new_sw:
                corpus = (wc.lemmatize(i))
                corpus_lst.append(wc.lemmatize(i))
                 
                return corpus
        
    # create fuction to get the subjectivity and polarity
    def getAnalysis(tweet):
        if sia.polarity_scores(tweet)["compound"] < 0:
            return 'NEGATIVE'
        elif sia.polarity_scores(tweet)["compound"]==0:
            return 'NEUTRAL'
        else:
            return 'POSITIVE'
    
    
    def pos_neg_words(clear_tweet):
        pos_senti = []
        neg_senti = []
        words = clear_tweet.split()
        
        for word in words:
            sc = sia.polarity_scores(word)
            if sc['compound'] > 0:
                pos_senti.append(word)
            elif sc['compound'] < 0:
                neg_senti.append(word)
        return pos_senti, neg_senti
    
    
    df['ClearTweet'] = df['TRANSLATED_TWEET'].apply(cleaning_tweet)
    
    df['TRANSLATED_SENTIMENT_ANALYSIS_RESULT'] = df[df['ClearTweet'].notnull()]['ClearTweet'].apply(getAnalysis)
    df['TRANSLATED_POSITIVE_WORDS'] = df[df['ClearTweet'].notnull()]['ClearTweet'].apply(lambda twt:', '.join(pos_neg_words(twt)[0]))
    df['TRANSLATED_NEGATIVE_WORDS'] = df[df['ClearTweet'].notnull()]['ClearTweet'].apply(lambda twt:', '.join(pos_neg_words(twt)[1]))
    print(df['TRANSLATED_TWEET'])
    print('METHOD 2 ANALYSIS completed')
    
  
    
    df['USER_LOCATION'] = df['USER_LOCATION'].apply(lambda x: x.upper())
    df['SOURCE'] = df['SOURCE'].apply(lambda x: x.upper())
    df['TWEET_URL'] = df['TWEET_ID'].apply(lambda x:'https://twitter.com/twitter/statuses/'+'{}'.format(x))
    convert_datatype = {'DATE':str,'TIME':str,'TWEET_ID':str,'USER_ID':str,'USER_FOLLOWERS':str,'LIKES':str,
                        'RETWEET_COUNT':str, 'TWEET_GEO_COORDINATES':str}
    
    df = df.astype(convert_datatype)
    
    #df = df[['TWEET_ID', 'DATE', 'TIME', 'USER_ID', 'USER_SCREEN_NAME', 'USER_LOCATION', 'USER_FOLLOWERS', 
    #    'SOURCE', 'TWEET', 'LIKES', 'RETWEET_COUNT', 'USER_MENTIONS', 'HASTAGS_IN_TWEET', 'URLS_ATTACHED',
    #    'EMOJI_IN_TWEET', 'NAME', 'CONSTITUENCY', 'TAGGED_PERSON_PARTY', 'IS_RETWEET', 'TWEET_URL', 
    #    'TWEET_GEO_LOCATION', 'TWEET_GEO_COORDINATES']]
    
    df.drop_duplicates('TWEET_ID',inplace=True)
    df.replace([None],'',inplace=True)
    df.fillna('',inplace=True)
    print(df)
    
    # d1 = pd.read_excel('ALL_PARTY_TIMELINE_TWEETS.xlsx')
    # final_df = pd.concat([df,d1]) 
    
    # df.to_excel('ALL_PARTY_LEADERS_TIMELINE_TWEETS.xlsx',index=False, encoding='utf-16')
    
    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    today1 = str(today)
    yesterday1 = str(yesterday)
    old_data = pd.read_sql("select * from TW_BY_LEADERS_TIMELINE where PUBLISHED_DATE in ('{}', '{}')".format(today1,yesterday1),con)
    df = df.loc[~df['TWEET_ID'].isin(old_data['TWEET_ID'])]
    df.reset_index(inplace=True)
    cursor = con.cursor()
    cursor1 = con.cursor()
    for i in range(len(df)):
        data1 = [[df['NAME'][i], df['TWEET_ID'][i], df['DATE'][i], df['TIME'][i],
              df['USER_ID'][i], df['USER_SCREEN_NAME'][i], df['USER_LOCATION'][i],
              df['USER_FOLLOWERS'][i], df['SOURCE'][i], df['TWEET'][i], df['LIKES'][i], 
	      df['RETWEET_COUNT'][i],df['USER_MENTIONS'][i], df['HASTAGS_IN_TWEET'][i], 
	      df['URLS_ATTACHED'][i],df['EMOJI_IN_TWEET'][i],df['CONSTITUENCY'][i], 
	      df['TAGGED_PERSON_PARTY'][i], df['IS_RETWEET'][i],'KEERTHI', 'KEERTHI',
	      df['TWEET_URL'][i], df['TWEET_GEO_LOCATION'][i], df['TWEET_GEO_COORDINATES'][i],
	      df['POSITIVE_WORDS'][i],df['NEGATIVE_WORDS'][i],df['SENTIMENT_ANALYSIS'][i],
     	      df['TRANSLATED_TWEET'][i],df['TRANSLATED_POSITIVE_WORDS'][i],df['TRANSLATED_NEGATIVE_WORDS'][i],
	      df['TRANSLATED_SENTIMENT_ANALYSIS_RESULT'][i]]]
        # print(data1)
        cursor.prepare("""
                        INSERT INTO TW_BY_LEADERS_TIMELINE(CANDIDATE_NAME, TWEET_ID, PUBLISHED_DATE, TWEET_TIME,
                        USER_ID, USER_SCREEN_NAME, USER_LOCATION, USER_FOLLOWERS, DEVICE_SOURCE, 
                        TWEET_CONTENT, LIKES_COUNT, RETWEET_COUNT, USER_MENTIONS, HASTAGS_IN_TWEET, URLS_ATTACHED, 
                        EMOJI_IN_TWEET, CONSTITUENCY_NAME, CANDIDATE_PARTY_NAME, IS_RETWEET, CREATE_BY, EDIT_BY,
                        TWEET_URL, TWEET_GEO_LOCATION, TWEET_GEO_COORDINATES, TWEET_POSITIVE_WORDS, TWEET_NEGATIVE_WORDS,
		        TWEET_SENTIMENT_RESULT, TRANSLATED_TWEET, TRANSLATED_POSITIVE_WORDS, TRANSLATED_NEGATIVE_WORDS,
		        TRANSLATED_SENTIMENT_RESULT) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,
			:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30)
                       """)
        cursor.executemany(None, data1)
        con.commit()    

        

        # cursor1.prepare("""
        #                 DELETE FROM TW_TIMELINE_TW_SENT_ANALYSIS
        #         WHERE rowid not in
        #         (SELECT MIN(rowid)
        #         FROM TW_TIMELINE_TW_SENT_ANALYSIS
        #         GROUP BY TWEET_ID""")

        cursor1.execute("""DELETE FROM TW_BY_LEADERS_TIMELINE
                WHERE rowid not in
                (SELECT MIN(rowid)
                FROM TW_BY_LEADERS_TIMELINE
                GROUP BY TWEET_ID)
                """)
        con.commit()

    cursor.close()
    cursor1.close()
    con.close()
        
    

schedule.every(30).minutes.do(twitter_df)
#schedule.every().day.at("11:30").do(twitter_df)
while True:
    schedule.run_pending()
    time.sleep(1)
# twitter_df()

# df.drop(['created_at'],axis=1,inplace=True)
