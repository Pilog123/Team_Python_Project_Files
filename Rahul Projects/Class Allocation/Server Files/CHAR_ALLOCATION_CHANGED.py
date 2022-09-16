import pandas as pd
import numpy as np
import re
import time
from datetime import datetime
# from tqdm import tqdm, tqdm_notebook,tqdm_pandas
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()
import warnings
warnings.filterwarnings('ignore')

# ==========================================
# ===================================================================

def char_allocation(clause_data,property_cndtn_data,class_data,UOM,UOM1):
    start_time = datetime.now()
    clauseData = clause_data
    property_cndtn_data = property_cndtn_data
    classData = class_data
    classData.rename(columns={'RECORD_NO':'MDRM','LONG_TEXT':'Long Description','CLASS':'Class'},inplace=True)
    classData.drop_duplicates(inplace = True,keep=False,ignore_index=True)
    classData.columns
# =============================================================================================================
    def property_extraction(des,cls):
        
        prop_name = []
        des1 = des
        for pcls,prop,cndtn_chk,ptype in zip(property_cndtn_data['CLASS'],property_cndtn_data['PROPERTY'],property_cndtn_data['CONDITION_CHECK'], property_cndtn_data['TYPE']):
            if cls == pcls and ptype == 'REGEXP':
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip()])
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'CLAUSE':
                for cl_name,cl_val in zip(clauseData['CLAUSE_NAME'],clauseData['CLAUSE_VALUE']):
                    if cndtn_chk == cl_name:
                        if cl_val in des1:
                            if re.search(r'\b{}\b'.format(str(cl_val)),des1) != None:
                                prop_name.append([prop,re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip()])
                                des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'NORMAL':
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip()])
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)
            else:
                pass
            
        prop_list = []
        for i in prop_name:
            if i not in prop_list:
                prop_list.append(i)         
        return prop_list, des1
# =============================================================================================================
    classData['Properties'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[0],axis=1)
    classData.fillna('', inplace = True, axis = 1)
    classData['Prop_Name'] = ''
    classData['Prop_Val'] = ''
    classData['Quantity'] = classData['Properties'].apply(lambda x : len(x))
    class_data_new = classData.loc[classData.index.repeat(classData.Quantity)].reset_index(drop=True)
    # print(cls_df_new)
    name = []
    val = []
    for i in range(len(classData)):
        for j in range(len(classData['Properties'][i])):
            name.append(classData['Properties'][i][j][0])
            val.append(classData['Properties'][i][j][1])
    # print(name)
    # print(val)
    # print(len(name))
    # print(len(val))
    class_data_new['Prop_Name'] = name
    class_data_new['Prop_Val'] = val
    # cls_df_new.to_excel('cls_df_18_24_jan_final.xlsx', index = False)
    # file = pd.ExcelFile('ISML UOM_1.xls')
    UOM =  UOM
    UOM1 = UOM1
    def uom_val(des):
    
        prop_name = []
        for i in UOM['RULE_UOM'].unique():
            if re.search(r'\b([0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3})(\s|\S|)' + '{}''{}'.format(i,r'\b'),str(des)):
                val = (re.search(r'\b([0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3})(\s|\S|)' + '{}''{}'.format(i,r'\b'),str(des)).group())
                val1 = re.sub('[\.\[\]]','',str(val)).strip()
                val2 = re.sub('[0-9](\s|)','',str(val1)).strip()
                for j in UOM1['RULE_UOM'].unique():
                    if re.search(r'\b(^{}$)\b'.format(str(val2)), str(j)):
                        val3 = (re.search(r'\b(^{}$)\b'.format(str(val2)), str(j)).group())
                        prop_name.append(val3)             
            else:
                None
        prop_list = []
        for j in prop_name:
            if j not in prop_list:
                prop_list.append(j)
        # print(prop_list)
        if len(prop_list) != 0:
            return max(prop_list) #, prop_new
        else:
            return None
# ======================================================================================================================
    class_data_new1 = class_data_new
    class_data_new1['uom'] = class_data_new1.apply(lambda x: uom_val(x['Prop_Val']),axis=1)
    class_data_new1['uom'].value_counts()
    class_data_new1['uom'] = class_data_new1['uom'].apply(lambda x: re.sub(r'\[\]', '', str(x)))
    class_data_new1['uom'].replace('None', '', inplace = True)   
    class_data_new1['Prop_Val'] = class_data_new1.apply(lambda x: x['Prop_Val'].replace(x['uom'], ''), axis = 1)
    class_data_new1['Prop_Val'] = class_data_new1.apply(lambda x: x['Prop_Val'].replace(x['Prop_Name'], ''), axis = 1)
    class_data_new1['uom'].value_counts()
# ==============================================================================================================================
    group = class_data_new1.groupby(['MDRM'])
    final_data = pd.DataFrame()
    final_data
    for i in (class_data_new1['MDRM'].unique()):
        batch = group.get_group(i)
        batch['Prop_Val'] = batch.groupby(['Prop_Name'])['Prop_Val'].transform(lambda x : ' ; '.join(x))
        batch['uom'] = batch.groupby(['Prop_Name'])['uom'].transform(lambda x : ','.join(x))
        batch.drop_duplicates(subset = ['Prop_Val'], inplace = True, keep = 'first')
        final_data = pd.concat([final_data, batch], ignore_index = True)
    final_data.drop(['Properties','Quantity'], axis = 1, inplace = True)
    final_data.columns
    final_data.fillna('', inplace = True, axis = 1)
    final_data['uom'] = final_data['uom'].apply(lambda x: ','.join(list(pd.unique(x.strip().split(',')))))
    final_data['uom'] = final_data.apply(lambda x: re.sub(r'^,|,$','',x['uom']), axis = 1)
    final_data['Prop_Val'] = final_data.apply(lambda x: re.sub(r'^[\,\:]{1,2}|[\,\:]{1,2}$','',x['Prop_Val']), axis = 1)
    # final_df.to_excel('final_df_18_24jan_final.xlsx', index = False)
    final_data['Prop_Val'] = final_data.apply(lambda x: re.sub('[a-wy-zA-WY-Z]', '',x['Prop_Val']).replace(' ', '') if x['uom'] != '' else x['Prop_Val'], axis = 1)    
    final_data['Prop_Val'] = final_data['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x)))
    final_data['Prop_Val'] = final_data['Prop_Val'].apply(lambda x: ', '.join([re.sub('(^X|X$|^\sX)', '', i).strip() for i in str(x).split(';')]))
    # final_df.to_excel('output_char_extraction1_18_24.01.22_final.xlsx', index = False)
    final_data.rename(columns={'MDRM':'RECORD_NO','Long Description':'LONG_TEXT','Class':'CLASS','Prop_Val':'PROPERTY_VALUE_DATA','Prop_Name':'PROPERTY_NAME_DATA','uom':'UOM_DATA'},inplace=True)
    end_time = datetime.now()
    print("Time taken : {}".format(end_time - start_time))
    # print(final_df)
    return final_data
