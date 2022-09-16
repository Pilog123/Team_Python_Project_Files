from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.by import By
from selenium import webdriver
from time import sleep
from datetime import datetime
import pandas as pd
import time
import re
import cx_Oracle
from pytz import timezone 
from datetime import timedelta
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.chrome import ChromeDriverManager
#driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))

def check_candidate_name(line):
    candidate_names = pd.read_excel(r"/root/Desktop/Team Python Deployment/Gopi Projects/DH_RPA_Validations/DHS_RPA_Validations/online_news/Candidate List.xlsx",sheet_name='Sheet1')
    for i in candidate_names['WINNER CANDIDATE']:
        if re.search('\b{}\b'.format(i.lower()),line.lower()):
            return True
            break
def update_data(data1):

    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    cursor = con.cursor()
    for i in range(len(data1)):
        data2 = [[data1['media_name'][i],data1['source_image'][i],data1['head_line'][i],data1['source_url'][i],
        data1['published_date'][i],data1['published_time'][i]]]
        print(data2)
        cursor.prepare("""INSERT INTO NP_LIVE(MEDIA_NAME,SOURCE_IMG,HEAD_LINE,
    SOURCE_URL,PUBLISHED_DATE,PUBLISHED_TIME) VALUES (:0,:1,:2,:3,:4,:5)""")
        cursor.executemany(None, data2)
        con.commit()
    cursor.close()
    con.close()
    print('done')

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
    while True:
        try:

        # search.send_keys('{} when:1h'.format(name))
            for k in ['Indian national congress, telangana when:1h','Bharatiya Janata Party, Telangana when:1h','Telangana Rashtra Samithi when:1h','politics telangana when:1h']:
                driver.get('https://news.google.com/home?hl=en-IN&gl=IN&ceid=IN:en&v2prv=1')
                time.sleep(10)
                search = driver.find_element(By.XPATH,"//input[contains(@class,'Ax4B8 ZAGvjd')]")
                search.send_keys(k)
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

                data1.drop(['date_time'],axis=1,inplace=True)
                live_news = pd.concat([data1,live_news],ignore_index=True)
                print(data1)
                update_data(data1)
                    


            else:
                pass
        except:
            pass

  

        time.sleep(300)

get_live_news_data()
