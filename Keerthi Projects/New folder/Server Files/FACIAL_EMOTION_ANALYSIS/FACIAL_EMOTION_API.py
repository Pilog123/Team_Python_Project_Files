# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 11:01:01 2022

@author: Keerthi.P
"""

from pytube import YouTube
from fer import FER

import pandas as pd
import cv2

import glob
import os
from datetime import datetime



def facial_emotion_recognizer(url,fpers):
        
    start_time = datetime.now()

    videourl = url

    yt = YouTube(videourl)
    yt = yt.streams.filter(progressive=True, file_extension='mp4').order_by('resolution').desc().first()
    # if not os.path.exists(path):
    #     os.makedirs(path)
    video = yt.download()


    files = glob.glob('./facial_images/*')
    for f in files:
        os.remove(f)
    files1 = glob.glob('./facial_emotions_output/*')
    for f1 in files1:
        os.remove(f1)

        
    vidcap = cv2.VideoCapture(video)
    def getFrame(sec):
        vidcap.set(cv2.CAP_PROP_POS_MSEC,sec*1000)
        hasFrames,image = vidcap.read()
        if hasFrames:
            cv2.imwrite("./facial_images/image"+str(count)+".jpg", image)     # save frame as JPG file
        return hasFrames, image
    
    emo = []
    frames = []
    bin_imgs = []
    # op_imgs = []
    cnt = []
    sec = 0
    fps = int(fpers)
    frameRate = fps #//it will capture image in each 2 second
    count=1
    sum = 1
    success = getFrame(sec)
    while success:
        try:
            
            sec = sec + frameRate
            sec = round(sec, 2)
            success = getFrame(sec)
    
            input_image = success[1]
    
            # test_image = plt.imread("/content/Merlin_1.jpg")
            # Faces by default are detected using OpenCV's Haar Cascade classifier. To use the more accurate MTCNN network, add the parameter
            fer = FER(mtcnn = True)
            # Capture all the emotions on the image
    
        #     for i in img_path:
            result = fer.detect_emotions(input_image)
            
            
            for i in range(len(result)):
                bounding_box = result[i]["box"]
                emotions = result[i]["emotions"]
                cv2.rectangle(input_image,(
                int(bounding_box[0]), int(bounding_box[1])),(
                int(bounding_box[0] + bounding_box[2]), int(bounding_box[1] + bounding_box[3])),
                            (0, 155, 255), 2,)
                emotion_name, score = fer.top_emotion(input_image )
                for index, (emotion_name, score) in enumerate(emotions.items()):
    #                 color = (211, 211,211) if score < 0.01 else (0, 155, 255)
                    color = (0, 0, 255)
                    emotion_score = "{}: {}".format(emotion_name, "{:.2f}".format(score))

                    cv2.putText(input_image,emotion_score,
                            (bounding_box[0], bounding_box[1] + bounding_box[3] + 30 + index * 15),
                            cv2.FONT_HERSHEY_SIMPLEX,0.5,color,2,cv2.LINE_AA,)

                #Save the result in new image file
                cv2.imwrite("./facial_emotions_output/emotion"+str(count)+".jpg", input_image)

                # Print all captured emotions with the image
                emo.append(result)
                frames.append('FRAME {}'.format(count))
                
                with open("./facial_emotions_output/emotion"+str(count)+".jpg", 'rb') as f:
                    imgdata = f.read()
                bin_imgs.append(imgdata)

                # op_imgs.append("./facial_emotions_output/emotion"+str(count)+".jpg")
                
        #         print(result)
                # plt.imshow(test_image)
                cnt.append(sum)
                sum = sum + 1
            count = count + 1
        except:
            break
            
    print(len(bin_imgs))
    fer_emo = []
    for i in range(0,len(emo)):
        if len(emo[i]) == 0: 
            fer_emo.append({'ANGRY': 0.0,
                            'DISGUST': 0.0,
                            'FEAR': 0.0,
                            'HAPPY': 0.0,
                            'SAD': 0.0,
                            'SURPRISE': 0.0,
                            'NEUTRAL': 0.0})
        else:
            fer_emo.append(dict((k.upper(), v) for k, v in emo[i][0]['emotions'].items()))
    fer_df = pd.DataFrame(fer_emo)
    fer_df['VIDEOURL'] = videourl
    fer_df['FRAME_IN_SECONDS'] = frameRate
    fer_df['FRAME_NAME'] = frames
    fer_df['EMOTION_IMAGE'] = bin_imgs
    fer_df['INDEX'] = cnt
    fer_df = fer_df[['FRAME_NAME','VIDEOURL','ANGRY', 'DISGUST', 'FEAR', 'HAPPY', 'SAD', 'SURPRISE', 'NEUTRAL',
                     'EMOTION_IMAGE','FRAME_IN_SECONDS','INDEX']]
    
    convert_datatype = {'ANGRY':str,'DISGUST':str,'FEAR':str,'HAPPY':str,'SAD':str,
                        'SURPRISE':str, 'NEUTRAL':str,'FRAME_IN_SECONDS':str,'INDEX':str}
    
    fer_df = fer_df.astype(convert_datatype)
    print(fer_df)

    end_time = datetime.now()
    print("Time taken : {}".format(end_time - start_time))
    # streamer.filter(tweet_fields = ['referenced_tweets'])
    fer_df.to_excel('FACIAL_EMOTION_RESULT.xlsx', index=False)
    return fer_df

# @app.post("/facial_emotion_analysis/")
# async def facial_emotion_analysis(URL: str = Form(...), TIME_LAPSE: str = Form(...)):

#     # def connection(URL, TIME_LAPSE): 
#     #     return facial_emotion_recognizer(URL, TIME_LAPSE) 
#     # def allocation(tableName, colsArray, accessName):
#     #     con = create_engine('oracle+cx_oracle://{}'.format(accessName))       
#     #     print('SELECT {} FROM {}'.format(colsArray, tableName))
#     #     df = pd.read_sql('SELECT {} FROM {}'.format(colsArray, tableName) ,con)[:100]
        
#     #     return df
            
# # def connection(URL, TIME_LAPSE):
# #         return facial_emotion_analysis(URL, TIME_LAPSE) 
    
#     #print(DF)
#     return facial_emotion_recognizer(URL, TIME_LAPSE)
    
# @app.get("/facial_emotion_analysis/")
# def main():
# #     content = """
# # <body>
# # <form action="/uploadfiles/" enctype="multipart/form-data" method="post">
# # <input name="file" type="file" multiple>
# # <input type="submit">
# # </form>
# # </body>
# #     """
#     return #HTMLResponse(content=content)

# if __name__ == '__main__':
#     uvicorn.run("FACIAL_EMOTION_API:app", port= 8811)