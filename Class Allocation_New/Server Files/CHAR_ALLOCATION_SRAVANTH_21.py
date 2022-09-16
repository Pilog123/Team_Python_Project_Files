import pandas as pd
import numpy as np
import re
import time
from datetime import datetime
# from tqdm import tqdm, tqdm_notebook,tqdm_pandas
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()

# =============================================================================================================

def char_allocation(clause_data,property_cndtn_data,class_data,UOM,UOM1, desc_cls_file, df):
    start_time = datetime.now()

    # clauseData = pd.read_excel("Clauses-24.02.22.xlsx", 'Sheet2').iloc[:,:5]
    # property_cndtn_data = pd.read_excel("Is_properties_condition_working_24.02.22.xls").iloc[:,:9]
    # classData = pd.read_excel("spirdataDemo.xls")
    # classData['Long Description'] = classData['Long Description'].str.upper()
    # classData.drop_duplicates(inplace = True, keep=False, ignore_index=True)
    # classData.columns

    start_time = datetime.now()
    clauseData = clause_data
    property_cndtn_data = property_cndtn_data
    classData = class_data
    classData.rename(columns={'PART_REF_ISSUED': 'MDRM', 'ITEM_DESC': 'Long Description', 'OBJ_QUAL': 'Class'}, inplace=True)
    classData['Long Description'] = classData['Long Description'].str.upper()
    classData.drop_duplicates(inplace=True, keep='first', ignore_index=True)

    #-------------------------------------------------------------------------------------------------------------------
    # df = pd.read_excel('Is_properties_condition_working_21.03.22_rev1.xls', 'Sheet5')
    df.drop_duplicates(inplace=True, keep='first', ignore_index=True)
    print(df)
    #-------------------------------------------------------------------------------------------------------------------

    # classData.columns

    def cleanDescription(cls, desc_cls_file, des1):
        # -------------------------------------------------------------------------
        if cls in desc_cls_file.Class.tolist():

            cls_idx_lst = desc_cls_file.index[desc_cls_file['Class'] == cls].tolist()

            for cls_idx in cls_idx_lst:

                if desc_cls_file['Value'][cls_idx] in des1:
                    print('Orig Desc: ', des1, "|", 'Modified Desc: ', des1.replace(desc_cls_file['Value'][cls_idx], desc_cls_file['Value_replace'][cls_idx]), file=print_file)
                    des1 = des1.replace(desc_cls_file['Value'][cls_idx], desc_cls_file['Value_replace'][cls_idx])
            print('=' * 100, '\n', file=print_file)
        return des1
        # -------------------------------------------------------------------------

    # =============================================================================================================
    print_file = open('print_check.txt', 'w')
    def property_extraction(des,cls):
    
        prop_name = []


        
        des1 = des
        for pcls,prop,cndtn_chk,ptype in zip(property_cndtn_data['CLASS'],property_cndtn_data['PROPERTY'],property_cndtn_data['CONDITION_CHECK'], property_cndtn_data['TYPE']):

            if cls == pcls and ptype == 'REGEXP':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip()])
                    print(cndtn_chk)
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'CLAUSE':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                for cl_name,cl_val in zip(clauseData['CLAUSE_NAME'],clauseData['CLAUSE_VALUE']):
                    if cndtn_chk == cl_name:
                        if cl_val in des1:
                            if re.search(r'\b{}\b'.format(str(cl_val)),des1) != None:
                                prop_name.append([prop,re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip()])
                                des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'NORMAL':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip()])
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)
            else:
                pass
            
        prop_list = []
        for i in prop_name:
            if i not in prop_list:
                prop_list.append(i)

        print(prop_list)
        print(des1, ' | ', des)
        return prop_list, des

# =============================================================================================================

    classData['Properties'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[0],axis=1)

    classData.fillna('', inplace = True, axis = 1)

    classData['Prop_Name'] = ''
    classData['Prop_Val'] = ''

    classData['Quantity'] = classData['Properties'].apply(lambda x : len(x))

    classData_z = classData[classData['Quantity'] == 0]
    classData_z.reset_index(drop= True, inplace = True)

    classData_n = classData[classData['Quantity'] != 0]
    classData_n.reset_index(drop= True, inplace = True)

    classData_new = classData_n.loc[classData_n.index.repeat(classData_n.Quantity)].reset_index(drop=True)
    # classData_new

    # classData_new = pd.concat([classData_z, classData_1], ignore_index = True)
    # classData_new

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


    classData_new['Prop_Name'] = name
    classData_new['Prop_Val'] = val

    classData_new['Prop_Val'] = classData_new.apply(lambda x: re.sub(r'^[\,\:]{1,2}|[\,\:]{1,2}$','',x['Prop_Val']), axis = 1)

    # classData_new.to_excel('classData_24.02.22U.xlsx', index = False)

    # file = pd.ExcelFile('ISML UOM_1.xls')
    # UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
    # UOM
    #
    # UOM1 = pd.read_excel(file, 'uom')
    # UOM1

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
        
        # print(prop_list)

        if len(prop_list) != 0:
            return max(prop_list) #, prop_new
        else:
            return None


# =======================================================================================================================

    classData_new1 = classData_new

    classData_new1['uom_status'] = ''
    # cls_df_new

    classData_new1.loc[
        (classData_new1['Class'].isin(list(df['CLASS']))) & (classData_new1['Prop_Name'].isin(list(df['PROPERTY']))), [
            'uom_status']] = 1
    classData_new1.loc[
        ~((classData_new1['Class'].isin(list(df['CLASS']))) & (classData_new1['Prop_Name'].isin(list(df['PROPERTY'])))), [
            'uom_status']] = 0

    # classData_new1['uom'] = classData_new1.apply(lambda x: uom_val(x['Prop_Val']),axis=1)

    classData_new1['uom'] = classData_new1.apply(lambda x: uom_val(x['Prop_Val']) if x['uom_status'] == 1 else '', axis=1)

    # cls_df_new1['uom'].fillna('', inplace=True)

    classData_new1['uom_status'].value_counts()

    classData_new1['uom'].value_counts()

    classData_new1['uom'] = classData_new1['uom'].apply(lambda x: re.sub(r'\[\]', '', str(x)))

    classData_new1['uom'].replace('None', '', inplace = True)
    
    classData_new1['Prop_Val'] = classData_new1.apply(lambda x: x['Prop_Val'].replace(x['uom'], ''), axis = 1)
    
    classData_new1['Prop_Val'] = classData_new1.apply(lambda x: x['Prop_Val'].replace(x['Prop_Name'], ''), axis = 1)
    
    classData_new1['uom'].value_counts()


# ==============================================================================================================================

    group = classData_new1.groupby(['MDRM'])

    final_df = pd.DataFrame()
    # final_df

    for i in (classData_new1['MDRM'].unique()):
        batch = group.get_group(i)
        batch['Prop_Val'] = batch.groupby(['Prop_Name'])['Prop_Val'].transform(lambda x : ' ; '.join(x))
        batch['uom'] = batch.groupby(['Prop_Name'])['uom'].transform(lambda x : ','.join(x))
        batch.drop_duplicates(subset = ['Prop_Val'], inplace = True, keep = 'first')
        final_df = pd.concat([final_df, batch], ignore_index = True)



    # final_df.columns
    final_df.fillna('', inplace = True, axis = 1)

    final_df['uom'] = final_df['uom'].apply(lambda x: ','.join(list(pd.unique(x.strip().split(',')))))

    final_df['uom'] = final_df.apply(lambda x: re.sub(r'^,|,$','',x['uom']), axis = 1)

    final_df['uom'] = final_df.apply(lambda x: x['uom'].replace(r'"', 'INCH'), axis=1)

    final_df['Prop_Val'] = final_df.apply(lambda x: re.sub(r'^[\.\,\:\-\/()+]{1,3}|[\.\,\:\-\/()+]{1,3}$','',x['Prop_Val']), axis = 1)
    
    # final_df.to_excel('final_df_04.02.22.xlsx', index = False)

    final_df['Prop_Val'] = final_df.apply(lambda x: re.sub('[a-wy-zA-WY-Z]', '',x['Prop_Val']).replace(' ', ' ') if x['uom'] != '' else re.sub('^[T]', '',x['Prop_Val']) , axis = 1)
    
    final_df['Prop_Val'] = final_df['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x)))
    final_df['Prop_Val'] = final_df['Prop_Val'].apply(lambda x: ', '.join([re.sub('(^X|X$|^\sX)', '', i).strip() for i in str(x).split(';')]))
    
    final_df['Prop_Val'] = final_df.apply(lambda x: re.sub(r'^[\.\,\:\-\/()+T]{1,3}|[\.\,\:\-\/()+]{1,3}$','',x['Prop_Val']), axis = 1)
    
    final_df = pd.concat([classData_z, final_df], ignore_index = True)
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


    final_df.drop(['Properties','Quantity', 'uom_status'], axis = 1, inplace = True)
    final_df.to_excel('output_char_extraction_spir_exclude_data_23.03.22.xlsx', index = False)


    end_time = datetime.now()
    print("Time taken : {}".format(end_time - start_time))
    print(final_df)
    # return final_df.fillna('')
    return final_df


# char_allocation()



# dff['Long Description_r'] = dff.apply(lambda x: x['Long Description'].replace(x['Class'], '').replace(x['Prop_Name'], '').replace(x['Prop_Val'], '').replace(x['uom'], ''), axis = 1)