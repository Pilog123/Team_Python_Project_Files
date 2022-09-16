from nltk.sentiment import SentimentIntensityAnalyzer
from googleapiclient.discovery import build
from textblob import TextBlob
import pandas as pd
import numpy as np
import translators as ts
import pandas as pd
import schedule
import time
import datetime
import re
import cx_Oracle
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from datetime import timedelta

nltk.download('vader_lexicon')
# api_key = 'AIzaSyCHAi04Kiu5YktK7jm43NjrpOAwu8sGEP8'
# api_key = 'AIzaSyA_S2aOTosBt6d4z144kuQuPIwPy4jLgh8'
api_key = 'AIzaSyCgG1sSCMOMwzG_SuAyx_GFtMepXh0hf0M'
# api_key = 'AIzaSyBIGWEroXsadlJ31WBEnM36TMs6KKCrvPw'
youtube = build('youtube', 'v3', developerKey=api_key)

def text_prep(x: str) -> list:
    lemma = WordNetLemmatizer()
    sw = stopwords.words('english')
    nsw=["not","couldn't", 'didn', "didn't", 'doesn', "doesn't", 'hadn', "hadn't", 'hasn', "hasn't", 'haven', "haven't", 'isn',
         "isn't", 'ma', 'mightn', "mightn't", 'mustn', "mustn't", 'needn', "needn't", 'shan', "shan't", 'shouldn', "shouldn't",
         'wasn', "wasn't", 'weren', "weren't", 'won', "won't", 'wouldn', "wouldn't","don't","dont",'cant',"can't"]
    new_sw = []
    for word in sw:
        if(word not in nsw):
            new_sw.append(word)
    corp = str(x).lower() 
    corp = re.sub(r'["!@#\$%\^&*\(\)\[\]\{\};:,./<>?\|`~\-=_+0-9]', " ", corp)
    corp = corp.split()
#     tokens = word_tokenize(corp)
    words = [t for t in corp if t not in new_sw]
    lemmatize = [lemma.lemmatize(w) for w in words]
    return lemmatize

def pos_neg_words(clear_tweet):
        sia = SentimentIntensityAnalyzer()
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

def find_pol(review):
    return TextBlob(review).sentiment.polarity


def video_comments():
    date_today = str(datetime.date.today())
    yesterday = str(datetime.date.today()-timedelta(days=1))
    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    video_data1 = pd.read_sql("select * from yt_nc_analysis where PUBLISHED_DATE= '{}' ".format(yesterday),con)
    video_data2 = pd.read_sql("select * from yt_channel_analysis where PUBLISHED_DATE= '{}' ".format(yesterday),con)
    video_data = pd.concat([video_data1,video_data2],ignore_index=True)
    replies = []
    com = []
    video_url = []
    video_title = []
    video_published = []
    media_channel = []
    commented_user = []
    for k,i,j in zip(video_data['VIDEO_TITLE'],video_data['SOURCE_URL'],video_data['MEDIA_CHANNEL_NAME']):
        video_ids = re.sub('=','',re.search('[=].*',i)[0])
        try:
            video_response=youtube.commentThreads().list(part='snippet,replies',videoId=video_ids).execute()
            for item in video_response['items']:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                com.append(comment)
                user_name = item['snippet']['topLevelComment']['snippet']['authorDisplayName']
                commented_user.append(user_name)
                if item['snippet']['totalReplyCount']>0:
                    reply1 = [item['replies']['comments'][i]['snippet']['textDisplay'] for i in range(len(item['replies']['comments']))]
                    replies.append(reply1)
                    print(reply1)
                else:
                    replies.append('')
                video_url.append('https://www.youtube.com/watch?v={}'.format(item['snippet']['videoId']))
                video_published.append(item['snippet']['topLevelComment']['snippet']['publishedAt'])
                video_title.append(k)
                media_channel.append(j)
        except:
            pass
    video_comments1 = pd.DataFrame(zip(video_title,media_channel,com,replies,video_url,video_published,commented_user),columns = ['video_title','media_channel','comments','replies','video_url','video_published','user_name'])
    video_comments1['translated_comments'] = video_comments1['comments'].apply(lambda x:ts.google(x, to_language='en'))
    video_comments1['preprocess_txt'] = video_comments1['comments'].apply(lambda x:text_prep(x))
    video_comments1['TITLE_TOTAL_LENGTH'] = video_comments1['preprocess_txt'].map(lambda x: len(x))
    file1 = pd.read_excel('Sentiment_Words.xlsx')
    neg_words = list(file1['Negative_Words'])
    pos_words = list(file1['Positive_Words'])
    num_pos = video_comments1['preprocess_txt'].map(lambda x: len([i for i in x if i in pos_words]))
    video_comments1['POSITIVE_ANALYSIS'] = num_pos
    num_neg = video_comments1['preprocess_txt'].map(lambda x: len([i for i in x if i in neg_words]))
    video_comments1['NEGATIVE_ANALYSIS'] = num_neg
    video_comments1['NEUTRAL_ANALYSIS'] = video_comments1['TITLE_TOTAL_LENGTH'] - (video_comments1['POSITIVE_ANALYSIS'] + video_comments1['NEGATIVE_ANALYSIS'])
    video_comments1['OVERALL_SENTIMENT_ANALYSIS'] = round((video_comments1['POSITIVE_ANALYSIS'] - video_comments1['NEGATIVE_ANALYSIS']) / video_comments1['TITLE_TOTAL_LENGTH'], 2)
    sent = []
    for i in video_comments1['OVERALL_SENTIMENT_ANALYSIS']:
        if i > 0:
            sent.append('POSITIVE')
        elif i < 0:
            sent.append('NEGATIVE')
        else:
            sent.append('NEUTRAL')
    video_comments1['Sentiment Score'] = sent
    video_comments1['VIDEO_PUBLISHED_DATE'] = pd.to_datetime(video_comments1['video_published']).dt.date
    video_comments1['replies'] = video_comments1['replies'].apply(lambda x:','.join(x))
    video_comments1['TRANSLATED_POSITIVE_WORDS'] = video_comments1['translated_comments'].apply(lambda twt:','.join(pos_neg_words(twt)[0]))
    video_comments1['TRANSLATED_NEGATIVE_WORDS'] = video_comments1['translated_comments'].apply(lambda twt:','.join(pos_neg_words(twt)[1]))
    video_comments1.drop_duplicates(['video_title','media_channel','comments','user_name'],keep='first',inplace=True)
    video_comments1.reset_index(drop=True,inplace=True)
    cursor = con.cursor()
    for i in range(len(video_comments1)):
        try:
            data2 = [[video_comments1['video_title'][i].upper(),video_comments1['media_channel'][i],video_comments1['VIDEO_PUBLISHED_DATE'][i],
            video_comments1['comments'][i],video_comments1['replies'][i],
            video_comments1['video_url'][i],video_comments1['translated_comments'][i],
            video_comments1['Sentiment Score'][i],video_comments1['TRANSLATED_POSITIVE_WORDS'][i],video_comments1['TRANSLATED_NEGATIVE_WORDS'][i],video_comments1['user_name'][i],]]
            print(data2)
            cursor.prepare("""INSERT INTO YT_ANALYSIS_COMMENTS(VIDEO_TITLE,MEDIA_CHANNEL_NAME,PUBLISHED_DATE,VIDEO_COMMENTS,
            VIDEO_REPLIES,SOURCE_URL,VIDEO_TRANSLATED_COMMENTS,OVERALL_SENTIMENT_ANALYSIS,TRANSLATED_POSITIVE_WORDS,TRANSLATED_NEGATIVE_WORDS,COMMENTED_USER) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)""")
            cursor.executemany(None, data2)
            con.commit()
        except:
            pass
    cursor.close()
    con.close()
    print(video_comments1)
# video_comments()
schedule.every().day.at("10:41").do(video_comments)

while True:
    schedule.run_pending()
    time.sleep(1)
