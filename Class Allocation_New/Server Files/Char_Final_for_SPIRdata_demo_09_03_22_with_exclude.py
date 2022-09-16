import pandas as pd
import numpy as np
import re
import time
from datetime import datetime
# from tqdm import tqdm, tqdm_notebook,tqdm_pandas
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()

# =============================================================================================================

def char_allocation(char_data):
    char_data.rename(columns={'SPIR_NO': 'MDRM', 'ITEM_DESC': 'Long Description', 'OBJ_QUAL': 'Class'}, inplace=True)
    print(char_data)
    start_time = datetime.now()
    print(start_time)
    clause_df = pd.read_excel("Clauses-24.02.22.xlsx", 'Sheet2').iloc[:,:5]
    prop_cndtn_df = pd.read_excel("Is_properties_condition_working_24.02.22.xls").iloc[:,:9]
    # cls_df = pd.read_excel("spirdataDemo.xls")
    cls_df = char_data
    cls_df['Long Description'] = cls_df['Long Description'].str.upper()
    cls_df.drop_duplicates(inplace = True, keep=False, ignore_index=True)
    cls_df.columns

# =============================================================================================================
    
    def property_extraction(des,cls):
    
        prop_name = []
        
        des1 = des
        for pcls,prop,cndtn_chk,ptype in zip(prop_cndtn_df['CLASS'],prop_cndtn_df['PROPERTY'],prop_cndtn_df['CONDITION_CHECK'], prop_cndtn_df['TYPE']):
            if cls == pcls and ptype == 'REGEXP':
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip()])
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'CLAUSE':
                for cl_name,cl_val in zip(clause_df['CLAUSE_NAME'],clause_df['CLAUSE_VALUE']):
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

    cls_df['Properties'] = cls_df.apply(lambda x: property_extraction(x['Long Description'],x.Class)[0],axis=1)

    cls_df.fillna('', inplace = True, axis = 1)

    cls_df['Prop_Name'] = ''
    cls_df['Prop_Val'] = ''

    cls_df['Quantity'] = cls_df['Properties'].apply(lambda x : len(x))

    cls_df_z = cls_df[cls_df['Quantity'] == 0]
    cls_df_z.reset_index(drop= True, inplace = True)

    cls_df_n = cls_df[cls_df['Quantity'] != 0]
    cls_df_n.reset_index(drop= True, inplace = True)

    cls_df_new = cls_df_n.loc[cls_df_n.index.repeat(cls_df_n.Quantity)].reset_index(drop=True)
    cls_df_new

    # cls_df_new = pd.concat([cls_df_z, cls_df_1], ignore_index = True)
    # cls_df_new

    name = []
    val = []
    for i in range(len(cls_df)):
        for j in range(len(cls_df['Properties'][i])):
            name.append(cls_df['Properties'][i][j][0])
            val.append(cls_df['Properties'][i][j][1])

    # print(name)
    # print(val)

    print(len(name))
    print(len(val))


    cls_df_new['Prop_Name'] = name
    cls_df_new['Prop_Val'] = val

    cls_df_new['Prop_Val'] = cls_df_new.apply(lambda x: re.sub(r'^[\,\:]{1,2}|[\,\:]{1,2}$','',x['Prop_Val']), axis = 1)

    # cls_df_new.to_excel('cls_df_24.02.22U.xlsx', index = False)

    file = pd.ExcelFile('ISML UOM_1.xls')
    UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
    UOM

    UOM1 = pd.read_excel(file, 'uom')
    UOM1

# =================================================================================================================================

    def uom_val(des):
        
        prop_name = []

        for i in UOM['RULE_UOM'].unique():

            if re.search(r'\b([0-9]{1,4}[\/][0-9]{1,4}|[0-9]{1,4}[X][0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3}|[0-9]{1,4})' + '{}''{}'.format(i,r'\b'),str(des)):
                val = (re.search(r'\b([0-9]{1,4}[\/][0-9]{1,4}|[0-9]{1,4}[X][0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3}|[0-9]{1,4})' + '{}''{}'.format(i,r'\b'),str(des)).group())
                
                val1 = re.sub('[\.\[\]]','',str(val)).strip()
                val2 = re.sub('([0-9]{1,4}[\/][0-9]{1,4}|[0-9]{1,4}[X][0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3}|[0-9]{1,4})','',str(val1)).strip()
                
                for j in UOM1['RULE_UOM'].unique():
                
                    if re.search(r'\b(^{}$)\b'.format(str(val2)), str(j)):
                        val3 = (re.search(r'\b(^{}$)\b'.format(str(val2)), str(j)).group())
                        prop_name.append(val3)        
                    elif re.search(r'(^{}$)'.format(str(val2)), str(j)):
                        val3 = (re.search(r'(^{}$)'.format(str(j)), str(val2)).group())
                        prop_name.append(val3)   

            elif re.search(r'([0-9]{1,4}[\/][0-9]{1,4}|[0-9]{1,4}[X][0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3}|[0-9]{1,4})(\s|\S|)' + 
                        '{}'.format(i),str(des)):
                val = (re.search(r'([0-9]{1,4}[\/][0-9]{1,4}|[0-9]{1,4}[X][0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3}|[0-9]{1,4})(\s|\S|)' + 
                        '{}'.format(i),str(des)).group())
                
                val1 = re.sub('[\.\[\]]','',str(val)).strip()
                val2 = re.sub('([0-9]{1,4}[\/][0-9]{1,4}|[0-9]{1,4}[X][0-9]{1,4}|[0-9]{1,4}[.][0-9]{1,3}|[0-9]{1,4})','',str(val1)).strip()
                
                for j in UOM1['RULE_UOM'].unique():    
                    if re.search(r'\b(^{}$)\b'.format(str(j)), str(val2)):
                        val3 = (re.search(r'\b(^{}$)\b'.format(str(j)), str(val2)).group())
                        prop_name.append(val3)
                    elif re.search(r'(^{}$)'.format(str(val2)), str(j)):
                        val3 = (re.search(r'(^{}$)'.format(str(j)), str(val2)).group())
                        prop_name.append(val3)               
            else:
                None
        
        prop_list = []

        for j in prop_name:
            if j not in prop_list:
                prop_list.append(j)
        
        print(prop_list)

        if len(prop_list) != 0:
            return max(prop_list) #, prop_new
        else:
            return None


# =======================================================================================================================

    cls_df_new1 = cls_df_new

    cls_df_new1['uom'] = cls_df_new1.apply(lambda x: uom_val(x['Prop_Val']),axis=1)

    cls_df_new1['uom'].value_counts()

    cls_df_new1['uom'] = cls_df_new1['uom'].apply(lambda x: re.sub(r'\[\]', '', str(x)))

    cls_df_new1['uom'].replace('None', '', inplace = True)
    
    cls_df_new1['Prop_Val'] = cls_df_new1.apply(lambda x: x['Prop_Val'].replace(x['uom'], ''), axis = 1)
    
    cls_df_new1['Prop_Val'] = cls_df_new1.apply(lambda x: x['Prop_Val'].replace(x['Prop_Name'], ''), axis = 1)
    
    cls_df_new1['uom'].value_counts()


# ==============================================================================================================================

    group = cls_df_new1.groupby(['MDRM'])

    final_df = pd.DataFrame()
    final_df

    for i in (cls_df_new1['MDRM'].unique()):
        batch = group.get_group(i)
        batch['Prop_Val'] = batch.groupby(['Prop_Name'])['Prop_Val'].transform(lambda x : ' ; '.join(x))
        batch['uom'] = batch.groupby(['Prop_Name'])['uom'].transform(lambda x : ','.join(x))
        batch.drop_duplicates(subset = ['Prop_Val'], inplace = True, keep = 'first')
        final_df = pd.concat([final_df, batch], ignore_index = True)



    final_df.columns
    final_df.fillna('', inplace = True, axis = 1)

    final_df['uom'] = final_df['uom'].apply(lambda x: ','.join(list(pd.unique(x.strip().split(',')))))

    final_df['uom'] = final_df.apply(lambda x: re.sub(r'^,|,$','',x['uom']), axis = 1)

    final_df['Prop_Val'] = final_df.apply(lambda x: re.sub(r'^[\.\,\:\-\/()+]{1,3}|[\.\,\:\-\/()+]{1,3}$','',x['Prop_Val']), axis = 1)
    
    # final_df.to_excel('final_df_04.02.22.xlsx', index = False)

    final_df['Prop_Val'] = final_df.apply(lambda x: re.sub('[a-wy-zA-WY-Z]', '',x['Prop_Val']).replace(' ', '') if x['uom'] != '' else x['Prop_Val'], axis = 1)
    
    final_df['Prop_Val'] = final_df['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x)))
    final_df['Prop_Val'] = final_df['Prop_Val'].apply(lambda x: ', '.join([re.sub('(^X|X$|^\sX)', '', i).strip() for i in str(x).split(';')]))
    
    final_df['Prop_Val'] = final_df.apply(lambda x: re.sub(r'^[\.\,\:\-\/()+]{1,3}|[\.\,\:\-\/()+]{1,3}$','',x['Prop_Val']), axis = 1)
    
    final_df = pd.concat([cls_df_z, final_df], ignore_index = True)
    final_df.fillna('', inplace = True, axis = 1)


    final_df['LONG_DESCRIPTION_EXCLUDE'] = ''

    for i in range(len(final_df)):

        res = ' '.join([ele for ele in final_df['Long Description'][i].split(' ') if(ele not in final_df['Class'][i].split(', '))])
    #     print(res)
        res1 = ' '.join([ele for ele in res.split(' ') if(ele not in final_df['Prop_Name'][i].split(', '))])
    #     print(res1)
        res2 = ' '.join([ele for ele in res1.split(' ') if(ele not in final_df['Prop_Val'][i].split(', '))])
    #     print(res2)
        res3 = ' '.join([ele for ele in res2.split(' ') if(ele not in final_df['uom'][i].split(', '))])
    #     print(res3)
        res4 = ' '.join([ele for ele in res3.split(' ') if(ele not in final_df['Prop_Val'][i] + final_df['uom'][i])])
    #     print(res4)
        final_df['LONG_DESCRIPTION_EXCLUDE'][i] = res4
    
    final_df.drop(['Properties','Quantity'], axis = 1, inplace = True)
    final_df.to_excel('output_char_extraction_spir_exclude_data_08.03.22.xlsx', index = False)

    end_time = datetime.now()
    print("Time taken : {}".format(end_time - start_time))
    print(final_df)
    return final_df


# char_allocation()



# dff['Long Description_r'] = dff.apply(lambda x: x['Long Description'].replace(x['Class'], '').replace(x['Prop_Name'], '').replace(x['Prop_Val'], '').replace(x['uom'], ''), axis = 1)