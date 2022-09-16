#importing the libraries

import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
import re
import cx_Oracle
from fastapi import FastAPI 
import uvicorn
from fastapi import Form
from typing import Optional


def po_percentage(req_dct, source_data, colsList, tableName, BATCH_ID):
    data_2 = dict()

    conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')
    # data = pd.read_sql("SELECT RECORD_NO, LONG_TEXT, BATCH_ID FROM O_RECORD_DATA_UNIFICATION WHERE BATCH_ID = '{}'".format(), conn)
    s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsList, tableName, BATCH_ID)
    print("SELECT RECORD_NO, LONG_TEXT, BATCH_ID FROM O_RECORD_DATA_UNIFICATION WHERE BATCH_ID in ('{}') ".format(req_dct['BATCH_ID']))
    data = pd.read_sql("SELECT RECORD_NO, LONG_TEXT, BATCH_ID FROM O_RECORD_DATA_UNIFICATION WHERE BATCH_ID in ('{}') ".format(req_dct['BATCH_ID']), conn)
    #---------------------------------------------------------------------------------------------------
    # data = pd.read_excel('DH_Match_PO.xlsx', 'Existing data')
    #---------------------------------------------------------------------------------------------------

    data = data.loc[data['LONG_TEXT'].isnull()==False,['RECORD_NO', 'LONG_TEXT']].reset_index(drop=True)

    sample=data.copy()

    LONG_TEXT_SPLIT = sample['LONG_TEXT'].apply(lambda x: re.split('[,:;\s]',str(x)))
    sample['LONG_TEXT_split'] = LONG_TEXT_SPLIT.apply(lambda x: [i for i in x if len(i)>1 ] )

    # data1 = pd.read_sql("SELECT * FROM O_RECORD_DESCRIPTION_MAPPING WHERE BATCH_ID = 'B00000000001279' AND ROWNUM < 3", conn)
    data1 = source_data
    data1.rename(columns = {'LONG_TEXT':'LONG_DESCRIPTION'}, inplace = True)
    idata = data1.loc[data1['LONG_DESCRIPTION'].isnull()==False].reset_index(drop=True)
    idata.drop_duplicates('LONG_DESCRIPTION', keep = 'first', inplace = True)
    
    #sample.insert(4, 'INPUT MATERIAL LONG TEXT', '')
    
    c = 0

    data_3 = pd.DataFrame()
    #for idx,i in tqdm(enumerate(idata['LONG_DESCRIPTION'].unique())):
    for (i,j) in (zip(idata['RECORD_NO'], idata['LONG_DESCRIPTION'])): 
        #sample['INPUT_RECORD_NO'] = i
        sample['INPUT_LONG_DESCRIPTION'] = j
        #sample['INPUT MATERIAL LONG TEXT'] = i
        sample['Percentage']=''
        sample['Unmatched_words']=''
        
        test_li =re.split('[,:;\s]',str(j))
        test_list = [j for j in test_li if len(j)>1 ]
        
        # sample['Percentage']=sample['LONG_TEXT_split'].apply(lambda x:round(len(set(test_list) & set(x)) / float(len(set(test_list) | set(x))) * 100,ndigits=2))
        sample['Percentage'] = sample['LONG_TEXT_split'].apply(
            lambda x: round(float(len(set(test_list) & set(x))) / float(len(set(test_list))) * 100, ndigits=2))

        sample['Unmatched_words']=sample['LONG_TEXT_split'].apply(lambda x:','.join(list(set(test_list).difference(set(x)))))
        
        sample_sorted=sample.sort_values(by='Percentage',ascending=False).head(2)
        sample_sorted.drop('LONG_TEXT_split', inplace=True, axis = 1)
        
        if sample_sorted.shape[0]>=1:
            sample_sorted1 = sample_sorted[sample_sorted['Percentage'] >= 50]
            c += 1
            #sample_sorted.append(idata.iloc[idx])
            sample_sorted1['Group']='G-{}'.format(c)
            # data_3['{}'.format(i)] = sample_sorted.to_dict(orient='records')
            # data_3 = sample_sorted1
            #print('--->', c)
            #data_2=pd.concat([sample_sorted,data_2],ignore_index=True)
            # data_3 = pd.concat([data_3, sample_sorted1], ignore_index=True)
            data_2['{}'.format(i)] = sample_sorted1.to_dict(orient='records')
            data_3 = pd.concat([data_3, sample_sorted1], ignore_index=True)
        else:
            pass
    #print(data_2)
    data_3.to_excel('po_final_output.xlsx', index=False)
    return data_2

