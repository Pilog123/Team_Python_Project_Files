import cx_Oracle
import re
import schedule
import time
import pytz
import pandas as pd
from newscatcherapi import NewsCatcherApiClient
from textblob import TextBlob
from datetime import datetime
from datetime import date
from datetime import timedelta
t = time.localtime()
UTC = pytz.utc
IST = pytz.timezone('Asia/Kolkata')

print('START')
def getSubjectivity(text):
    return TextBlob(text).sentiment.subjectivity

def getPolarity(text):
    return TextBlob(text).sentiment.polarity

def getAnalysis(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'
def candidate_name_check(line):
    candidate_name1 = pd.read_excel("Candidate List.xlsx",sheet_name='Sheet1')
    for i in candidate_name1['WINNER CANDIDATE']:
        if re.search('{}'.format(i.lower()),line.lower()):
            return True
            break


def online_news_data():

    print("IST in Default Format : ", 
      datetime.now(IST))
    newscatcherapi = NewsCatcherApiClient(x_api_key='2yk1lZUvbDHFb_Xj3hwNHz5KB3kt4RnuC7UCIbHI1OY')
    tody_news_data = pd.DataFrame()
    candidate_name = pd.read_excel("Candidate List.xlsx",sheet_name='Sheet2')

    for i,j,z in zip(candidate_name['WINNER CANDIDATE'],candidate_name['WINNER PARTY'],candidate_name['ASSEMBLY_CONSTITUENCY']):
 
        all_articles = newscatcherapi.get_search_all_articles(q=f'{i} and telangana',
                                                lang='en',
                                                countries='IN',
                                                page_size=100,by='day',from_=str(date.today()-timedelta(days=1)),to_=str(date.today()))
        newscat = pd.DataFrame(all_articles['articles'])
        if newscat.shape[0]>=1:
            newscat['ASSEMBLY_CONSTITUENCY'] = z
            newscat['CANDIDATE_NAME']=i
            newscat['PARTY_NAME']=j
            tody_news_data = pd.concat([tody_news_data,newscat],ignore_index=True)

    tody_news_data1 = tody_news_data[['title','published_date','clean_url','link','excerpt','CANDIDATE_NAME','PARTY_NAME','ASSEMBLY_CONSTITUENCY']]
    tody_news_data1 = tody_news_data1.astype({'link':'str'})
    tody_news_data1['response'] = tody_news_data1['title'].apply(lambda x:candidate_name_check(x))
    tody_news_data1 = tody_news_data1.loc[tody_news_data1['response']==True,:]
    tody_news_data1['TextBlob_Subjectivity'] =  tody_news_data1['title'].apply(getSubjectivity)
    tody_news_data1['TextBlob_Polarity'] = tody_news_data1['title'].apply(getPolarity)
    tody_news_data1['Sentiment_Analysis'] = tody_news_data1['TextBlob_Polarity'].apply(getAnalysis)
    #tody_news_data1['clean_url'] = tody_news_data1['clean_url'].apply(lambda x:re.sub('.com|.in','',x))
    tody_news_data1.fillna('NOT AVAILABLE',inplace=True)
    data = pd.read_excel('google_news_feed.xlsx')
    tody_news_data1['published_date'] =  pd.to_datetime(tody_news_data1['published_date']).dt.date
    tody_news_data1=tody_news_data1.astype({'published_date':str})
    tody_news_data1.reset_index(drop=True,inplace=True)
    data = pd.concat([tody_news_data1,data], ignore_index=True)
    data.to_excel('google_news_feed.xlsx', index=False)
    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    cursor = con.cursor()
    for i in range(len(tody_news_data1)):
        data1 = [[tody_news_data1['title'][i].upper(),tody_news_data1['clean_url'][i].upper(),tody_news_data1['published_date'][i],
                tody_news_data1['Sentiment_Analysis'][i].upper(),tody_news_data1['CANDIDATE_NAME'][i],tody_news_data1['PARTY_NAME'][i],
                tody_news_data1['ASSEMBLY_CONSTITUENCY'][i],'GOPI','GOPI',tody_news_data1['link'][i]]]
        cursor.prepare("""INSERT INTO NP_ANALYSIS(HEAD_LINE,MEDIA_NAME,PUBLISHED_DATE,SENTIMENT_ANALYSIS_RESULT,
        CANDIDATE_NAME,PARTY_NAME,CONSTITUENCY_NAME,CREATE_BY,EDIT_BY,SOURCE_URL) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9)""")
        cursor.executemany(None, data1)
        con.commit()
    print('inserting {} records into DXP_NEWSPAPER_ANALYSIS is done'.format(len(tody_news_data1)))
    cursor.close()
    con.close()

schedule.every().day.at("12:55").do(online_news_data)

while True:
    schedule.run_pending()
    time.sleep(1)
