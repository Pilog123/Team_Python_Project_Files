
import pandas as pd
from tqdm import tqdm
import cx_Oracle
import re
import warnings
warnings.filterwarnings('ignore')

def LONG_DES_CLASS_VALIDATION(class_d):
    # conn = cx_Oracle.connect('DR1024170/Pipl#mdrm_170@172.16.1.63:1521/DR100411')
    # class_d=pd.read_sql("SELECT RECORD_NO,CLASS_TERM,MASTER_COLUMN6 FROM O_RECORD_MASTER WHERE MASTER_COLUMN6 IS NOT NULL AND ROWNUM < 10000",conn)
    # conn.close()
    
    class_df=class_d
    class_df_1=class_df.loc[(class_df['CLASS_TERM'] != 'UNKNOWN')&(class_df['CLASS_TERM'] !='UNK'),:]
    class_df_1['changed_master_column'] = class_df_1['MASTER_COLUMN6'].apply(lambda x:re.sub('[^\s\w]',' ',x))
    group_data=class_df_1.groupby(by = 'changed_master_column') 
    # group_data=class_df_1.groupby(by = 'MASTER_COLUMN6') 
    indx_3=[]
    part_list=class_df_1['changed_master_column'].unique() 
    # part_list=class_df_1['MASTER_COLUMN6'].unique() 
    
    for parts in tqdm(part_list):
        parts_check = group_data.get_group(parts)
        if parts_check.shape[0]>1:
            parts_check.drop_duplicates(subset = ['CLASS_TERM'],keep='first' ,inplace = True)
        else:
            pass
        if parts_check.shape[0] > 1: 
            indx_3.extend(list(parts_check.index))
            
    long_des=class_df_1[class_df_1.index.isin(indx_3)].reset_index(drop=True)
    long_des=long_des.sort_values(by='changed_master_column').reset_index(drop=True)
    # long_des=long_des.sort_values(by='MASTER_COLUMN6').reset_index(drop=True)
    long_desc = long_des.loc[:,['RECORD_NO','CLASS_TERM','MASTER_COLUMN6']]
    # long_des.to_excel('test.xlsx',index=False)
    return long_desc
    
# LONG_DES_CLASS_VALIDATION()