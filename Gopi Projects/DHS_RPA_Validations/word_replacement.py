# import pandas as pd
# import re
# import numpy
# # from tqdm.notebook import tqdm_notebook
# # tqdm_notebook.pandas()


# # d = pd.ExcelFile('Replace Words.xlsx')
# # df = pd.read_excel(d,'Replace')
# # df
# def replace_cndtn(string,rep):
#     rep = str(rep)
#     rep = re.sub(r'[^A-Za-z0-9\s\.\/,;&:\'\|]',' ',rep).strip()
#     string = re.sub(r'[^A-Za-z0-9\s\.\/,;&:\'\|]',' ',string).strip()
#     st = string
#     for rep_trm in rep.split('|'):
#         if re.search(r'{}'.format(rep_trm),st):
#             des = re.sub(re.search(rep_trm,st).group(),'',st)
#             st = des
#     return st
# def replace_function(df):

#     df['After_Replacement'] = df.apply(lambda x:replace_cndtn(x.LONG_DESC,x.VALUE),axis=1)
#     df['After_Replacement'] = df.apply(lambda x:replace_cndtn(x.After_Replacement,x.CLASS),axis=1)
#     df['After_Replacement'] = df.apply(lambda x:replace_cndtn(x.After_Replacement,x.VENDOR_NAME),axis=1)
#     df['After_Replacement'] = df.apply(lambda x:replace_cndtn(x.After_Replacement,x.NAME),axis=1)
#     # print(df)
#     return df

# # replace_function(df)

import pandas as pd
import warnings
warnings.filterwarnings('ignore')
from tqdm import tqdm
import re


def po_percentage(data, data1):
    # data=pd.read_excel('des_gen_algo.xlsx')

    data = data.loc[data['TEXT'].isnull() == False, ['RECORD_NO', 'LOCALE', 'TEXT']].reset_index(drop=True)
    data['TEXT'] = data['TEXT'].apply(lambda x: re.sub('(, |; |: |,,|\")', ',', str(x)))
    data['TEXT'] = data['TEXT'].apply(lambda x: re.sub('(/ | / )', '/', str(x)))
    # data_2=pd.DataFrame()
    sample = data1
    sample['MASTER_COLUMN6'] = sample['MASTER_COLUMN6'].apply(lambda x: re.sub('(, |; |: |,,|\")', ',', str(x)))
    sample['MASTER_COLUMN6'] = sample['MASTER_COLUMN6'].apply(lambda x: re.sub('(/ | / )', '/', str(x)))
    sample['purchase_split'] = sample['MASTER_COLUMN6'].apply(lambda x: re.split('[,;:\s]', str(x)))
    data['unmatched_words'] = ''
    # data['split_words'] = ''
    for idx, (text, record) in tqdm(enumerate(zip(data['TEXT'], data['RECORD_NO']))):
        #     sample=data.loc[data['PURCHASE'] != i].reset_index(drop=True)
        # sample['Percentage']=''
        # sample['Unmatched_words']=''
        test_list = re.split('[,;:\s]', str(text))
        # print(test_list)
        long_desc = sample.loc[sample['RECORD_NO'] == record, ['purchase_split']].reset_index(drop=True)
        long_desc_split = long_desc['purchase_split'][0]
        if long_desc.shape[0] >= 1:
            # sample['Percentage']=sample['PURCHASE_split'].apply(lambda x:round(len(set(test_list) & set(x)) / float(len(set(test_list) | set(x))) * 100,ndigits=2))
            remaining_words = ','.join(list(set(long_desc_split).difference(set(test_list))))
            data['unmatched_words'][idx] = remaining_words
            # data['split_words'][idx] = test_list
        else:
            # data['split_words'][idx] = test_list
            data['unmatched_words'][idx] = ''
        # sample_sorted=sample.sort_values(by='Percentage',ascending=False).head(5)
        # if sample_sorted.shape[0]>1:
        # sample_sorted['Batch']='EE0{}'.format(idx)
        #     data_2=pd.concat([sample_sorted,data_2],ignore_index=True)
        # else:
        #     pass
    data['unmatched_words'] = data['unmatched_words'].apply(lambda x: re.sub('^,', '', str(x)))
    data['unmatched_words'] = data['unmatched_words'].apply(lambda x: re.sub(r'(\W){2,}', '', str(x)))
    sample = sample.loc[:, ['RECORD_NO', 'MASTER_COLUMN6']]
    data = data.merge(sample, on='RECORD_NO', how='left')
    data = data[['RECORD_NO', 'LOCALE', 'TEXT', 'MASTER_COLUMN6', 'unmatched_words']]
    return data
