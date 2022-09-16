import schedule
import time
import pytz
import re
import pandas as pd
import translators as ts
from datetime import date
from GoogleNews import GoogleNews
from textblob import TextBlob
from datetime import datetime
t = time.localtime()
UTC = pytz.utc
IST = pytz.timezone('Asia/Kolkata')
candidate = pd.read_excel(r"C:\Users\Rahul.S\Downloads\Candidate List.xlsx")
def candidate_name(line):
    for i in candidate['WINNER CANDIDATE']:
        if re.search(i,line):
            return i
            break

def online_news():
    print("IST in Default Format : ", 
      datetime.now(IST))
    googlenews = GoogleNews()
    for i in candidate['WINNER CANDIDATE']:
        googlenews.search(i)

    data = pd.DataFrame()
    for i in googlenews.results():
        data1 = pd.DataFrame(i,index=[0])
        data = pd.concat([data1,data],ignore_index=True)
    data['datetime'] = pd.to_datetime(data['datetime']).dt.date
    data = data.loc[data['datetime'] == str(date.today())]
    data['title'] = data['title'].apply(lambda x:ts.google(x, to_language='en'))
    data['desc'] = data['desc'].apply(lambda x:ts.google(x, to_language='en'))
    data = data[['title','media','datetime','desc']]

    def getSubjectivity(text):
        return TextBlob(text).sentiment.subjectivity
  

    def getPolarity(text):
        return TextBlob(text).sentiment.polarity

    data['TextBlob_Subjectivity'] =  data['title'].apply(getSubjectivity)
    data['TextBlob_Polarity'] = data['title'].apply(getPolarity)
    def getAnalysis(score):
        if score < 0:
            return 'Negative'
        elif score == 0:
            return 'Neutral'
        else:
            return 'Positive'
    data['TextBlob_Analysis'] = data['TextBlob_Polarity'].apply(getAnalysis)
    data1 = pd.read_excel(r"C:\Users\Rahul.S\Desktop\online_news\google_news_feed1.xlsx")
    data = data[['title','media','datetime','desc','TextBlob_Analysis']]
    data.fillna(datetime.now(),axis=0,inplace=True)
    data = pd.concat([data1,data], ignore_index=True)
    data['name'] = data['title'].apply(lambda x:candidate_name(x))
    data.to_excel('google_news_feed1.xlsx', index=False)

schedule.every().day.at("09:47").do(online_news)

while True:
    schedule.run_pending()
    time.sleep(1)
