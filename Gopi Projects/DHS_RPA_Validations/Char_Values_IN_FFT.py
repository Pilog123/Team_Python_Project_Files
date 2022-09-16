
import pandas as pd
import re
from tqdm import tqdm
import cx_Oracle



def CHAR_FFT_VALIDATION(df_char):

    # df_char.columns = ['RECORD_NO', 'PROPERTY_NAME', 'PROPERTY_VALUE1', 'VENDOR_NAME', 'TABLE_NAME']
    df_char.fillna('', inplace=True)
    df_char = df_char.loc[df_char['VALUE'] != '', :]
    # df_char['CHAR_VALUES'] = df_char.groupby(['RECORD_NO'])['PROPERTY_VALUE1'].transform(lambda x: ','.join(x))

    # df_char.drop_duplicates(subset=['RECORD_NO'], keep='first', inplace=True)
    ref_text = df_char.copy()
    ref_text.fillna('', axis=1, inplace=True)
    # ref_text.drop('PROPERTY_VALUE1', axis=1, inplace=True)
    ref_text['CHAR_VALUES_IN_FFT'] = ''
    ref_text.reset_index(drop=True, inplace=True)
    for ind, string in enumerate(tqdm(ref_text['VALUE'])):
        words = []
        values = string.split(',')
        values_1 = map(lambda x: re.sub(r'[\(\)+$^*\\?;\[\]\{\}=]', '', x), values)
        #     print(values_1)
        descr = re.sub(r'[\(\)+$^*\\?;\[\]\{\}=]', '', ref_text['FFT'][ind])
        inf_values = filter(lambda x: re.findall(r'\b({})\b'.format(x.strip()), descr), values_1)

        [words.append(i) for i in inf_values if i not in words]
        ref_text.loc[ref_text.index[ind]:ref_text.index[ind], 'CHAR_VALUES_IN_FFT'] = ','.join(words).strip()

    ref_text = ref_text.loc[ref_text['CHAR_VALUES_IN_FFT'] != '']
    # print(ref_text.columns)
    # ref_text.loc[:,['RECORD_NO','MASTER_COLUMN10','Char_Values','FFT_Duplicates_Char']].to_excel('test.xlsx',index=False)

    return ref_text

# CHAR_FFT_VALIDATION()
