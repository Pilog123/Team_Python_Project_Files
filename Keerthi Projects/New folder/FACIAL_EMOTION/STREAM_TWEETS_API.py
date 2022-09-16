

from datetime import datetime
from pytz import timezone
from fastapi import FastAPI
import FACIAL_EMOTION_API
import uvicorn
from fastapi import FastAPI, Form
from typing import Optional
import numpy as np
import pandas as pd
import cx_Oracle

import uvicorn

app = FastAPI()

@app.get('/FACIAL_EMOTION')
@app.post('/FACIAL_EMOTION')
async def main(URL = Form(...),TIME_LAPSE = Form(...)):

    new_df1=FACIAL_EMOTION_API.facial_emotion_recognizer(URL, TIME_LAPSE)

    con = cx_Oracle.connect("DR1024230/Pipl#mdrm$230@172.16.1.61:1521/DR101413")
    cursor = con.cursor()
    for i in range(len(new_df1)):
        data1 = [[new_df1['FRAME_NAME'][i],new_df1['VIDEOURL'][i],new_df1['ANGRY'][i],new_df1['DISGUST'][i],
                new_df1['FEAR'][i],new_df1['HAPPY'][i],new_df1['SAD'][i],
                new_df1['SURPRISE'][i],new_df1['NEUTRAL'][i],new_df1['IMG_INDEX'][i],new_df1['EMOTION_IMAGE'][i],
                new_df1['FRAME_IN_SECONDS'][i],'KESHAV_MGR','KESHAV_MGR']]
        # print(data1)
        cursor.prepare("""INSERT INTO IS_FACE_EMOTION_ANALYSIS(FRAME_NAME,VIDEOURL,ANGRY,DISGUST,FEAR,HAPPY,SAD,SURPRISE,NEUTRAL,
                        IMG_INDEX,EMOTION_IMAGE,FRAME_IN_SECONDS,CREATE_BY,EDIT_BY) 
        VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13)""")
        cursor.executemany(None, data1)
        con.commit()
    return {'Response':'DONE!'}



if __name__ == '__main__':
    uvicorn.run(app, host = '172.16.1.62',port= 6657, log_level = 'info')
