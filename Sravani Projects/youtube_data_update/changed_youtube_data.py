import schedule
import time
import nltk  
import datetime
import cx_Oracle
import re
import pandas as pd
import numpy as np
from tqdm import tqdm
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from googleapiclient.discovery import build
from datetime import timedelta
import translators as ts
from googletrans import Translator
translator = Translator()

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

def get_channel_stats(youtube, channel_ids):
    all_data = []
    request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
    response = request.execute() 
    
    for i in range(len(response['items'])):
        data = dict(Channel_name = response['items'][i]['snippet']['title'],
                    Subscribers = response['items'][0]['statistics']['hiddenSubscriberCount'],
                    VIDEO_VIEWS = response['items'][i]['statistics']['viewCount'],
                    Total_videos = response['items'][i]['statistics']['videoCount'],
                    playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        all_data.append(data)
    
    return all_data

def get_video_ids(youtube, playlist_id):
    
    request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId = playlist_id,
                maxResults = 50)
    response = request.execute()
    
    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids

def get_video_details(youtube, video_ids):
    today = datetime.date.today()
    all_video_stats = []
    for i in range(0, len(video_ids), 50):
        request = youtube.videos().list( part='snippet,statistics',id=','.join(video_ids[i:i+50]))
        response = request.execute()
        for idx,video in enumerate(response['items']):
            try:
                if str(re.sub(r'T.*','',video['snippet']['publishedAt'])) == str(today):
                    video_stats = dict(VIDEO_TITLE = video['snippet']['title'],
                                   VIDEO_PUBLISHED_DATE = video['snippet']['publishedAt'],
                                   VIDEO_VIEWS = video['statistics']['viewCount'],
                                   VIDEO_LIKES = video['statistics']['likeCount'],
                                   VIDEO_DISLIKES = video['statistics']['favoriteCount'],
                                   VIDEO_COMMENTS = video['statistics']['commentCount'],
                                   VIDEO_IDS = video_ids[idx])
                    all_video_stats.append(video_stats)
            except:
                pass
        return all_video_stats    

def filter_title(title):
    candidate_names = pd.read_excel('Candidate List V1.xls')
    for i in candidate_names['WINNER_CANDIDATE']:
#     for i in ['kcr','revanth reddy','bandi sanjay kumar','ktr']:
        if re.search('{}'.format(i.lower()),title.lower()):
            return [title,i.upper()]
        else:
            pass

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

ids = ['UCYgQ_QOT21HE1nI4a1KyM-A', #6tv
              'UCBnQF4yu8du8ypDFnqUDlwA',#4tv
              'UCfymZbh17_3T_UhgjkQ9fRQ',#10tv
              'UCl5YgCiwSRVOiC2Nd1P9v1A',#99tv
              'UC_2irx_BQR7RsBKmUV9fePQ',#ABN
              'UCs9H1cyB3OHdy8wkit8ZKg',#ETV Andhra Pradesh
              'UCS9H1cyB3OHdy8wkit8ZKg',#ETV Telangana
              'UCjeunQb9f4dP-krW8869s_w',#hmtv
              'UCumtYpCY26F6Jr3satUgMvA',#ntv
              'UCLF6LTfTX_eO-LeLbarKlvw',#Raj News
              'UCZ9m4KOh8Ei60428xeGYDCQ',#Sakshi TV
              'UCueYkfaSk8GbBeDXvCcQ_vQ',#Studio N
#              'UCu6edg8_eu3-A8ylgaWereA',#T News
              'UCO6-loIjSgmwk3PEUQ5ZM5Q',#Mojo TV
#               'UCAR3h_9fLV82N2FH4cE4RKw',#TV5 News
#              'UCPXTXMecYqnRKNdqdVOGSFg',#TV9 Telugu
              'UCDCMjD1XIAsCZsYHNMGVcog',#V6 News
              'UCrwE8kVqtIUVUzKui2WVpuQ'#BJP    
             ]
ids1 = ['UCDNhtX8lDoGwxchhOi8_gyQ',#TRS party
               'UC18m-lkuzbIyboBTVE6HDuw',#KTR
               'UCuYV0BBXUfdjqgzGcv7nblw',#Revanth Reddy
               'UCGhwLiDevyTcp6kngCvc6Qw',# Bandi Sanjay
               'UCOlAI11emHhX2nkQClHIkMg',#Gampa Govardhan
               'UCdFW2PDhHkDBWVrCpOWnokA',#Harish Rao
               'UCz98PJuBjnKlBTP_6PN6X7Q',#Gudem Mahipal Reddy
               'UClKvh9pfTCxsMrfU3CmR5eA',#ERRABELLI DAYAKAR RAO
               'UC4xNCfOgr9y_7H_n-BA9mhA',#JEEVAN REDDY THATIPARTHI
               'UCjKed-zRmKlJH_IWArdWwUg',#G. KISHAN REDDY
               'UCGXFlO3FkfoGq3xlXpuKB5Q',#PADAKANTI RAMADEVI
               'UCqGWCTNH80yVRZqT5lWa_eg',#MOHAMMED ALI SHABBIR
               'UCMJKDZnWrjq5yP_KfNJDgeQ',#VAKITI SUNITHA LAXMA REDDY
               'UCRkIqbs828IFdFkr8q6Ij1w',#GUNDE VIJAYA RAMA RAO
              'UCjfYRVmU3JrKN78mLJHHUPQ'#INC party
]
def youtube_comment_data(channel_ids,name):
    #api_key = 'AIzaSyCHAi04Kiu5YktK7jm43NjrpOAwu8sGEP8'
    api_key = 'AIzaSyCgG1sSCMOMwzG_SuAyx_GFtMepXh0hf0M'
    #api_key = 'AIzaSyBIGWEroXsadlJ31WBEnM36TMs6KKCrvPw'
    #api_key = 'AIzaSyA_S2aOTosBt6d4z144kuQuPIwPy4jLgh8'
    # youtube = build('youtube','v3',devloperKey = api_key)
    youtube = build('youtube', 'v3', developerKey=api_key)
    channel_statistics = get_channel_stats(youtube, channel_ids)
    channel_data = pd.DataFrame(channel_statistics)
    print(channel_data)
    vidData = pd.DataFrame()
    for i,j in tqdm(zip(channel_data['Channel_name'],channel_data['playlist_id'])):
        try:
            video_ids = get_video_ids(youtube, j)
            video_details=get_video_details(youtube, video_ids)
            video_data = pd.DataFrame(video_details)
            video_data['CHANNEL_NAME']= i
            vidData = pd.concat([vidData,video_data],ignore_index = True)
        except:
            pass
    print(vidData)
    if vidData.shape[0]>0:
        vidData['VIDEO_PUBLISHED_DATE'] = pd.to_datetime(vidData['VIDEO_PUBLISHED_DATE']).dt.date
        vidData['VIDEO_VIEWS'] = pd.to_numeric(vidData['VIDEO_VIEWS'])
        vidData['VIDEO_LIKES'] = pd.to_numeric(vidData['VIDEO_LIKES'])
        vidData['VIDEO_DISLIKES'] = pd.to_numeric(vidData['VIDEO_DISLIKES'])
        vidData['VIDEO_VIEWS'] = pd.to_numeric(vidData['VIDEO_VIEWS'])
        if name == 'NC':
            vidData['filter_title'] = vidData['VIDEO_TITLE'].apply(lambda x:filter_title(x))
            filter_data = vidData[vidData['filter_title'].notnull()]
            filter_data.reset_index(drop=True,inplace=True)
            filter_data['VIDEO_TITLE'] = filter_data['filter_title'].apply(lambda x:x[0] if x is not np.NaN else x)
            filter_data['CANDIDATE_NAME'] = filter_data['filter_title'].apply(lambda x:x[1] if x is not np.NaN else x)
        else:
            filter_data = vidData.copy()
        filter_data['preprocess_txt'] = filter_data['VIDEO_TITLE'].apply(lambda x:text_prep(x))
        filter_data['TITLE_TOTAL_LENGTH'] = filter_data['preprocess_txt'].map(lambda x: len(x))
        file1 = pd.read_excel('Sentiment_Words.xlsx')
        neg_words = list(file1['Negative_Words'])
        pos_words = list(file1['Positive_Words'])
        num_pos = filter_data['preprocess_txt'].map(lambda x: len([i for i in x if i in pos_words]))
        filter_data['POSITIVE_ANALYSIS'] = num_pos
        num_neg = filter_data['preprocess_txt'].map(lambda x: len([i for i in x if i in neg_words]))
        filter_data['NEGATIVE_ANALYSIS'] = num_neg
        filter_data['NEUTRAL_ANALYSIS'] = filter_data['TITLE_TOTAL_LENGTH'] - (filter_data['POSITIVE_ANALYSIS'] + filter_data['NEGATIVE_ANALYSIS'])
        filter_data['OVERALL_SENTIMENT_ANALYSIS'] = round((filter_data['POSITIVE_ANALYSIS'] - filter_data['NEGATIVE_ANALYSIS']) / filter_data['TITLE_TOTAL_LENGTH'], 2)
        sent = []
        for i in filter_data['OVERALL_SENTIMENT_ANALYSIS']:
            if i > 0:
                sent.append('POSITIVE')
            elif i < 0:
                sent.append('NEGATIVE')
            else:
                sent.append('NEUTRAL')
        filter_data['OVERALL_SENTIMENT_ANALYSIS'] = sent
        if name == 'NC':
            dict = {"KCR" : 'K. CHANDRASHEKAR RAO',"KTR" : 'K. T. RAMA RAo'}
            filter_data=filter_data.replace({"CANDIDATE_NAME": dict})
        # filter_data2.to_excel("youtube_final_data.xlsx",index=False)
        return filter_data


# nc_data = filter_data3[filter_data3['CHANNEL_NAME'].isin(['6TV','Studio N News','V6 News Telugu','Sakshi TV','99TV Telugu',	'4tv News Channel','Mojo TV Live','NTV Telugu',	'TRS Party',	
# 'Raj News Telugu','ABN Telugu','10TV News Telugu','Republic World','NDTV','India Today','TIMES NOW'])]

# channel_data = filter_data2[filter_data2['CHANNEL_NAME'].isin(['Kishan Reddy G.','Indian National Congress','VIJAYARAMA RAO G','Anumula Revanth Reddy','gampa govardhan','Jeevan Reddy Thatiparthi',
# 'MinisterKTR','Sunitha Laxma Reddy Vakiti','Harish Rao Tanneru','TRS Party','Bandi Sanjay Kumar','Mohammad Ali Shabbir','Gudem Mahipal Reddy','Bharatiya Janata Party','Errabelli Dayakar Rao',
# 'Dr. Padakanti Ramadevi'])]
# # data = data['Views'].astype(str)
def youtube_overall():
    nc_data = youtube_comment_data(ids,'NC')
    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    cursor = con.cursor()
    convert_datatype = {'VIDEO_VIEWS':str,'VIDEO_LIKES':str,'VIDEO_DISLIKES':str,'VIDEO_COMMENTS':str,
                'POSITIVE_ANALYSIS':str,'NEGATIVE_ANALYSIS':str,'VIDEO_PUBLISHED_DATE':str}
    today = datetime.date.today()
    old_nc_data = pd.read_sql("select * from yt_nc_analysis where published_date='{}'".format(today),con)
    nc_data = nc_data.astype(convert_datatype)
    nc_data['VIDEO_URL'] = nc_data['VIDEO_IDS'].apply(lambda x:'https://www.youtube.com/watch?v={}'.format(x))
    nc_data = nc_data.loc[~nc_data['VIDEO_URL'].isin(list(old_nc_data['SOURCE_URL']))]
    nc_data.drop_duplicates(['CHANNEL_NAME','VIDEO_TITLE','VIDEO_PUBLISHED_DATE'],inplace=True,ignore_index=True)
    nc_data.reset_index(drop=True,inplace=True)

    for i in tqdm(range(len(nc_data))):
        data1 = [[nc_data['CANDIDATE_NAME'][i],nc_data['CHANNEL_NAME'][i],nc_data['VIDEO_TITLE'][i].upper(),
            nc_data['VIDEO_PUBLISHED_DATE'][i],nc_data['VIDEO_VIEWS'][i],
            nc_data['VIDEO_LIKES'][i],nc_data['VIDEO_DISLIKES'][i],
            nc_data['VIDEO_COMMENTS'][i],nc_data['POSITIVE_ANALYSIS'][i],
            nc_data['NEGATIVE_ANALYSIS'][i],nc_data['OVERALL_SENTIMENT_ANALYSIS'][i],'SYS','SYS',nc_data['VIDEO_URL'][i]]]
        print(data1)
        cursor.prepare("""INSERT INTO YT_NC_ANALYSIS(CANDIDATE_NAME,MEDIA_CHANNEL_NAME,VIDEO_TITLE,PUBLISHED_DATE,
        VIDEO_VIEWS,VIDEO_LIKES,VIDEO_DISLIKES,VIDEO_COMMENTS_COUNT,POSITIVE_ANALYSIS,NEGATIVE_ANALYSIS,OVERALL_SENTIMENT_ANALYSIS,
        CREATE_BY,EDIT_BY,SOURCE_URL) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)""")
        cursor.executemany(None, data1)
        con.commit()
    
    channel_data = youtube_comment_data(ids1,'Channel')
    if (channel_data is not None) and (channel_data.shape[0]>0):
        channel_data['VIDEO_URL'] = channel_data['VIDEO_IDS'].apply(lambda x:'https://www.youtube.com/watch?v={}'.format(x))
        channel_data = channel_data.astype(convert_datatype)
        old_channel_data = pd.read_sql("select * from yt_nc_analysis where published_date='{}'".format(today),con)
        channel_data = channel_data.loc[~channel_data['VIDEO_URL'].isin(list(old_channel_data['SOURCE_URL']))]
        channel_data.reset_index(drop=True,inplace=True)

        for i in tqdm(range(len(channel_data))):
            data2 = [[channel_data['CHANNEL_NAME'][i],channel_data['VIDEO_TITLE'][i].upper(),
            channel_data['VIDEO_PUBLISHED_DATE'][i],channel_data['VIDEO_VIEWS'][i],
            channel_data['VIDEO_LIKES'][i],channel_data['VIDEO_DISLIKES'][i],
            channel_data['VIDEO_COMMENTS'][i],channel_data['POSITIVE_ANALYSIS'][i],
            channel_data['NEGATIVE_ANALYSIS'][i],channel_data['OVERALL_SENTIMENT_ANALYSIS'][i],'SYS','SYS',channel_data['VIDEO_URL'][i]]]
            print(data2)
            cursor.prepare("""INSERT INTO YT_CHANNEL_ANALYSIS(MEDIA_CHANNEL_NAME,VIDEO_TITLE,PUBLISHED_DATE,
            VIDEO_VIEWS,VIDEO_LIKES,VIDEO_DISLIKES,VIDEO_COMMENTS_COUNT,POSITIVE_ANALYSIS,NEGATIVE_ANALYSIS,OVERALL_SENTIMENT_ANALYSIS,
            CREATE_BY,EDIT_BY,SOURCE_URL) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12)""")
            cursor.executemany(None, data2)
            con.commit()
    # nc_data1 = nc_data.loc[nc_data['VIDEO_URL'].isin(list(old_nc_data['SOURCE_URL']))]
        channel_data1 = channel_data.loc[channel_data['VIDEO_URL'].isin(list(old_channel_data['SOURCE_URL']))]
        for k in range(len(channel_data1)):
            data3 = [[channel_data1['VIDEO_VIEWS'][k],channel_data1['VIDEO_LIKES'][k],
            channel_data1['VIDEO_DISLIKES'][k]]]
            cursor.execute("""UPDATE YT_CHANNEL_ANALYSIS SET VIDEO_VIEWS=:1,VIDEO_LIKES=:2,
            VIDEO_DISLIKES=:3 WHERE SOURCE_URL = '{}' """.format(channel_data1['VIDEO_URL'][k]),data3)
            con.commit()
    print("{} records inserted into YT_NC_ANALYSIS".format(len(nc_data)))
    #print("{} records inserted into YT_CHANNEL_ANALYSIS".format(len(channel_data)))

    cursor.close()
    con.close()
#youtube_comment_data()
schedule.every().day.at("16:37").do(youtube_overall)
schedule.every().day.at("12:43").do(youtube_overall)
while True:
    schedule.run_pending()
    time.sleep(1)

