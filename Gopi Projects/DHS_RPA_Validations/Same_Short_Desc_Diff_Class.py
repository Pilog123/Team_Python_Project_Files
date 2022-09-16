
import pandas as pd
from tqdm import tqdm
import cx_Oracle
import warnings
warnings.filterwarnings('ignore')

def SHORT_DESC_CLASS_VALIDATION(class_df):
    # conn = cx_Oracle.connect('DR1024170/Pipl#mdrm_170@172.16.1.63:1521/DR100411')
    # class_d=pd.read_sql("SELECT RECORD_NO,CLASS_TERM,MASTER_COLUMN5 FROM O_RECORD_MASTER WHERE MASTER_COLUMN5 IS NOT NULL  AND ROWNUM < 40000",conn)
    # conn.close()
    class_df_1=class_df.loc[(class_df['CLASS'] != 'UNKNOWN')&(class_df['CLASS'] !='UNK'),:]
    
    group_data=class_df_1.groupby(by ='SHORT_DESC') 
    indx_2=[]
    part_list=class_df_1['SHORT_DESC'].unique() 
    
    for parts in tqdm(part_list):
        parts_check = group_data.get_group(parts)
        if parts_check.shape[0]>1:
            parts_check.drop_duplicates(subset = ['CLASS'],keep='first' ,inplace = True)
        else:
            pass
        if parts_check.shape[0] > 1: 
            indx_2.extend(list(parts_check.index))
            
    shot_des=class_df_1[class_df_1.index.isin(indx_2)].reset_index(drop=True)
    shot_des=shot_des.sort_values(by='SHORT_DESC').reset_index(drop=True)
    # shot_des.to_excel('test.xlsx',index=False)
    return shot_des
# SHORT_DESC_CLASS_VALIDATION()   
