import cx_Oracle
import re
import schedule
import time
import pytz
import pandas as pd
from textblob import TextBlob
from datetime import datetime
from datetime import date
from datetime import timedelta
from pytz import timezone 
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver
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
def get_live_news_data():
    # driver = webdriver.Chrome(ChromeDriverManager().install())
    #    driver = webdriver.Firefox(GeckoDriverManager().install())
    live_news = pd.DataFrame(columns=['media_name','source_img','head_line','time','source_url'])
    driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    #driver1 = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
    time.sleep(10)
    # # driver.find_element(By.XPATH,"//button[contains(@class,'gb_pf gb_rf')]").click()
    # for name in tqdm(candidate_names['WINNER CANDIDATE'][:20]):
    # score_value = 0
    # local_timezone = tzlocal.get_localzone()
    # while True:
    tody_news_data = pd.DataFrame()
    candidate_name = pd.read_excel("Candidate List.xlsx",sheet_name='Sheet2')

    for i,j,z in zip(candidate_name['WINNER CANDIDATE'],candidate_name['WINNER PARTY'],candidate_name['ASSEMBLY_CONSTITUENCY']):

        # all_articles = newscatcherapi.get_search_all_articles(q=f'{i} and telangana',
        #                                         lang='en',
        #                                         countries='IN',
        #                                         page_size=100,by='day',from_=str(date.today()-timedelta(days=1)),to_=str(date.today()))
        # newscat = pd.DataFrame(all_articles['articles'])
    



# search.send_keys('{} when:1h'.format(name))
        driver.get('https://news.google.com/home?hl=en-IN&gl=IN&ceid=IN:en&v2prv=1')
        time.sleep(10)
        search = driver.find_element(By.XPATH,"//input[contains(@class,'Ax4B8 ZAGvjd')]")
        search.send_keys(f'{i} when:1d')
        search_button = driver.find_element(By.XPATH,"//button[contains(@class,'gb_mf')]")
        search_button.click()
        time.sleep(10)
        names1 = []
        time_data = []
        urls = []
        source = []
        head_line = []
        date_time = []
        check_value = ''

        try:
            time_data += list(map(lambda x:x.text,driver.find_elements(By.XPATH,"//time[contains(@class,'WW6dff uQIVzc Sksgp')]")))
            urls += list(map(lambda x:x.get_attribute('href'),driver.find_elements(By.XPATH,"//a[contains(@class,'VDXfz')]")))
            source += list(map(lambda x:x.find_element(By.TAG_NAME,'img').get_attribute('src'),driver.find_elements(By.XPATH,"//div[contains(@class,'wsLqz RD0gLb')]")))
            head_line += list(map(lambda x:x.text,driver.find_elements(By.XPATH,"//h3[contains(@class,'ipQwMb ekueJc RD0gLb')]")))
            names1 += list(map(lambda x:x.find_element(By.TAG_NAME,'a').get_attribute('text'),driver.find_elements(By.XPATH,"//div[contains(@class,'wsLqz RD0gLb')]")))
            date_time += list(map(lambda x:datetime.strptime(x.get_attribute('datetime'), "%Y-%m-%dT%H:%M:%S%z"),driver.find_elements(By.XPATH,"//time[contains(@class,'WW6dff uQIVzc Sksgp slhocf')]")))
        except:
            pass
        driver.find_element(By.XPATH,"//button[contains(@class,'gb_pf gb_rf')]").click()
        head_line1 = list(filter(lambda a: a != '',head_line))
        time_data1 = list(filter(lambda a: a != '',time_data))
        source1 = list(filter(lambda a: a != '',source))
        names2 = list(filter(lambda a: a != '',names1))
        date_time1 = list(filter(lambda a: a != '',date_time))

        urls1 = []
        for url in urls[-len(head_line1):]:
            driver.get(url)
            time.sleep(15)
            urls1.append(driver.current_url)
        print(len(urls),len(urls1),len(head_line1))

        if len(urls1)==len(head_line1):
            data = pd.DataFrame(zip(names2,source1,head_line1,urls1,date_time1),columns=['media_name','source_image','head_line','source_url','date_time'])
            data1 = pd.concat([live_news,data],ignore_index=False)
            data1.drop_duplicates(['head_line','media_name'],keep=False,inplace=True)
            data1 = data1.loc[data1['head_line']!='']
            data1.reset_index(drop=True,inplace=True)
            data1 = data1.loc[data1['date_time'].isnull() == False].reset_index(drop=True)
            if data1.shape[0] != 0:
                data1['date_time'] = pd.to_datetime(data1['date_time']).dt.tz_convert('Asia/Kolkata')
                print(data1['date_time'])
                data1['time'] = data1['date_time'].apply(lambda x:(datetime.now(timezone("Asia/Kolkata"))-timedelta(hours=1))-x)
                print(datetime.now(timezone("Asia/Kolkata"))-timedelta(hours=1))
                data1['time'] = data1['time'].apply(lambda x:str(int(x.total_seconds()/60))+' '+'minutes ago')
                data1['published_date'] = data1['date_time'].apply(lambda x:x.strftime('%Y-%m-%d'))
                data1['published_time'] = data1['date_time'].apply(lambda x:x.strftime('%H:%M:%S'))
        if data1.shape[0]>=1:
            data1['ASSEMBLY_CONSTITUENCY'] = z
            data1['CANDIDATE_NAME']=i
            data1['PARTY_NAME']=j
            tody_news_data = pd.concat([tody_news_data,data1],ignore_index=True)
    return tody_news_data
        # data1.drop(['date_time'],axis=1,inplace=True)
        # live_news = pd.concat([data1,live_news],ignore_index=True)
        # print(data1)
        # update_data(data1)
            

def online_news_data():

    print("IST in Default Format : ", 
      datetime.now(IST))
    # newscatcherapi = NewsCatcherApiClient(x_api_key='2yk1lZUvbDHFb_Xj3hwNHz5KB3kt4RnuC7UCIbHI1OY')

    tody_news_data = get_live_news_data()
    tody_news_data1 = tody_news_data[['head_line','published_date','media_name','source_url','CANDIDATE_NAME','PARTY_NAME','ASSEMBLY_CONSTITUENCY']]
    tody_news_data1 = tody_news_data1.astype({'source_url':'str'})
    tody_news_data1['response'] = tody_news_data1['head_line'].apply(lambda x:candidate_name_check(x))
    tody_news_data1 = tody_news_data1.loc[tody_news_data1['response']==True,:]
    tody_news_data1['TextBlob_Subjectivity'] =  tody_news_data1['head_line'].apply(getSubjectivity)
    tody_news_data1['TextBlob_Polarity'] = tody_news_data1['head_line'].apply(getPolarity)
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
        data1 = [[tody_news_data1['head_line'][i].upper(),tody_news_data1['media_name'][i].upper(),tody_news_data1['published_date'][i],
                tody_news_data1['Sentiment_Analysis'][i].upper(),tody_news_data1['CANDIDATE_NAME'][i],tody_news_data1['PARTY_NAME'][i],
                tody_news_data1['ASSEMBLY_CONSTITUENCY'][i],'SYS','SYS',tody_news_data1['source_url'][i]]]
        cursor.prepare("""INSERT INTO NP_ANALYSIS(HEAD_LINE,MEDIA_NAME,PUBLISHED_DATE,SENTIMENT_ANALYSIS_RESULT,
        CANDIDATE_NAME,PARTY_NAME,CONSTITUENCY_NAME,CREATE_BY,EDIT_BY,SOURCE_URL) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9)""")
        cursor.executemany(None, data1)
        con.commit()
    print('inserting {} records into DXP_NEWSPAPER_ANALYSIS is done'.format(len(tody_news_data1)))
    cursor.close()
    con.close()

schedule.every().day.at("15:07").do(online_news_data)

while True:
    schedule.run_pending()
    time.sleep(1)
