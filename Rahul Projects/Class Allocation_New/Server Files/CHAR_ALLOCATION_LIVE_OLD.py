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
    # classData.rename(columns={'PART_REF_ISSUED': 'MDRM', 'ITEM_DESC': 'Long Description', 'OBJ_QUAL': 'Class'}, inplace=True)
    classData.rename(columns={'REGISTERED_RECORD_NO': 'MDRM', 'LONG_TEXT': 'Long Description', 'CLASS': 'Class'}, inplace=True)

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

    def altr_des(des):
        des = re.sub(r' +',' ',des)
        des = re.sub(r'\n',' ',des)
        des = re.sub(r'( ,|, )',',',des)
        des = re.sub(r'( :|: )',':',des)
        des = re.sub(r'( ;|; )',';',des)
        des = re.sub(r'( -|- | - )','-',des)
        des = re.sub(r'( _|_ | _ )','_',des)
        des = re.sub(r'( \/|\/ )','/',des)
        x = des.upper()

    #     print(x)
        return x

    # =============================================================================================================
    print_file = open('print_check.txt', 'w')


    def property_extraction(des,cls):

        des = altr_des(des)
        prop_name = []
        des1 = des
        for pcls,prop,uom,rule_uom,cndtn_chk,ptype in zip(property_cndtn_data['CLASS'],
                                                        property_cndtn_data['PROPERTY'],
                                                        property_cndtn_data['UOM'],
                                                        property_cndtn_data['RULE_UOM'],
                                                        property_cndtn_data['CONDITION_CHECK'],
                                                        property_cndtn_data['TYPE']):

            if cls == pcls and ptype == 'REGEXP':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip(),uom,rule_uom])
                    # prop1.append(prop)
                    # prop1.append(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())
                    # prop1.append(uom)
                    # prop1.append(rule_uom)
                    # print(cndtn_chk)
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'CLAUSE':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                for cl_name,cl_val in zip(clauseData['CLAUSE_NAME'],clauseData['CLAUSE_VALUE']):
                                if cndtn_chk == cl_name:
                                    if cl_val in des1:
                                        if re.search(r'\b{}\b'.format(str(cl_val)),des1) != None:
                                            prop_name.append([prop,re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip(),uom,rule_uom])
                #                             prop1.append(prop)
                #                             prop1.append(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())
                #                             prop1.append(uom)
                #                             prop1.append(rule_uom)                                
                                            des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())), '', des1)

       

            elif cls == pcls and ptype == 'NORMAL':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip(),uom,rule_uom])
#                 prop1.append(prop)
#                 prop1.append(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())
#                 prop1.append(uom)
#                 prop1.append(rule_uom)
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)
            else:
                pass
            
        prop_list = []
        for i in prop_name:
            if i not in prop_list:
                prop_list.append(i)

        print(prop_list)
        print(des1, ' | ', des)
        # return prop_list, des
        return prop_list

# =============================================================================================================

    # classData['Properties'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[0],axis=1)
    classData['Properties'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class),axis=1)


    classData.fillna('', inplace = True, axis = 1)

    classData['Prop_Name'] = ''
    classData['Prop_Val'] = ''
    classData['uom'] = ''
    classData['r_uom'] = ''

    classData['Quantity'] = classData['Properties'].apply(lambda x : len(x))
    classData.reset_index(drop=True, inplace=True)

    classData_z = classData[classData['Quantity'] == 0]
    classData_z.reset_index(drop= True, inplace = True)

    classData_n = classData[classData['Quantity'] != 0]
    classData_n.reset_index(drop= True, inplace = True)

    classData_new = classData_n.loc[classData_n.index.repeat(classData_n.Quantity)].reset_index(drop=True)
    # classData_new

    # classData_new = pd.concat([classData_z, classData_1], ignore_index = True)
    # classData_new

    group = classData_new.groupby(['MDRM'])

    classData_new1 = pd.DataFrame(columns=['MDRM', 'Class', 'Long Description','Properties','Prop_Name','Prop_Val','uom','r_uom',
                                        'Quantity'])

    for i in (classData_new['MDRM'].unique()):
        batch = group.get_group(i)
        batch.reset_index(drop = True, inplace = True)  

        for j in range(len(batch)):
    #         print(batch['Properties'])

            batch['Prop_Name'][j] = batch['Properties'][j][j][0]
            batch['Prop_Val'][j] = batch['Properties'][j][j][1]
            batch['uom'][j] = batch['Properties'][j][j][2]
            batch['r_uom'][j] = batch['Properties'][j][j][3]
    #         print(batch)
    #         print("="*40)

        classData_new1 = pd.concat([classData_new1, batch], ignore_index = True)
        
    # classData_new1.columns
    classData_new1.fillna('', inplace = True, axis = 1)
    # classData_new1

    if len(classData_new1) != 0:    
    
        classData_new1['Prop_Val'] = classData_new1.apply(lambda x: re.sub(r'^[\.\,\:\-\/()+]{1,3}|[\.\,\:\-\/()]{1,3}$','',x['Prop_Val']), axis = 1)

        classData_new1['Prop_Val'] = classData_new1.apply(lambda x: re.sub(r'^[\,\:]{1,2}|[\,\:]{1,2}$','',x['Prop_Val']), axis = 1)

        classData_new1['Prop_Val'] = classData_new1['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x)))

        classData_new1['Prop_Val'] = classData_new1.apply(lambda x: x['Prop_Val'].replace(x['uom'], ''), axis = 1)

        classData_new1['Prop_Val'] = classData_new1.apply(lambda x: x['Prop_Val'].replace(x['Prop_Name'], ''), axis = 1)

        classData_new1['Prop_Val'] = classData_new1.apply(lambda x: x['Prop_Val'].replace(x['r_uom'], ''), axis = 1)

        classData_new1['Prop_Val'] = classData_new1.apply(lambda x: re.sub(x['r_uom'], '', x['Prop_Val']),axis=1)
        
        
    #     ==============================================================================================================

        group1 = classData_new1.groupby(['MDRM'])

        final_df = pd.DataFrame()
        # classData_new1

        for i in (classData_new1['MDRM'].unique()):
            batch1 = group1.get_group(i)
            batch1['Prop_Val'] = batch1.groupby(['Prop_Name'])['Prop_Val'].transform(lambda x : ' ; '.join(x))
            batch1['uom'] = batch1.groupby(['Prop_Name'])['uom'].transform(lambda x : ','.join(x))
            batch1.drop_duplicates(subset = ['Prop_Val'], inplace = True, keep = 'first')
            final_df = pd.concat([final_df, batch1], ignore_index = True)
            
    #     ==============================================================================================================

        # classData_new1.columns
        final_df.fillna('', inplace = True, axis = 1)

        final_df['Prop_Val'] = final_df.apply(lambda x: re.sub(r'^[\.\,\;\:\-\/()+]{1,3}|[\.\,\:\;\-\/()+]{1,3}$','',x['Prop_Val']), axis = 1)

        final_df['Prop_Val'] = final_df['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x)))

        final_df = pd.concat([classData_z, final_df], ignore_index = True)
        final_df.fillna('', inplace = True, axis = 1)
        
        #     ==============================================================================================================
        
        final_df['LONG_DESCRIPTION_EXCLUDE'] = ''

        for i in range(len(final_df)):

            res = ' '.join([ele for ele in final_df['Long Description'][i].split(' ') if(ele not in (final_df['Class'][i].split(',') or final_df['Class'][i].split(' ') or final_df['Class'][i].split(', ')))])
        #     print(res)
            res1 = ' '.join([ele for ele in res.split(' ') if(ele not in (final_df['Prop_Name'][i].split(',') or final_df['Prop_Name'][i].split(' ') or final_df['Prop_Name'][i].split(', ')))])
        #     print(res1)
            res2 = ' '.join([ele for ele in res1.split(' ') if(ele not in (final_df['Prop_Val'][i].split(',') or final_df['Prop_Val'][i].split(' ') or final_df['Prop_Val'][i].split(', ')))])
        #     print(res2)
            res3 = ' '.join([ele for ele in res2.split(' ') if(ele not in (final_df['uom'][i].split(',') or final_df['uom'][i].split(' ') or final_df['uom'][i].split(', ')))])
        #     print(res3)
            res4 = ' '.join([ele for ele in res3.split(' ') if(ele not in (final_df['r_uom'][i].split('|') or final_df['r_uom'][i].split(' ') or final_df['r_uom'][i].split('| ')))])

            res5 = ' '.join([ele for ele in res4.split(' ') if(ele not in final_df['Prop_Val'][i] + final_df['uom'][i] + final_df['r_uom'][i])])
        #     print(res4)

            final_df['LONG_DESCRIPTION_EXCLUDE'][i] = res5
            
        # final_df.drop(['Properties','Quantity','r_uom'], axis = 1, inplace = True)
        
    else:
        final_df = classData
        # final_df.drop(['Properties','Quantity','r_uom'], axis = 1, inplace = True)

    # print(name)
    # print(val)

    final_df.to_excel('output_char_extraction_spir_exclude_data_23.03.22.xlsx', index = False)

    end_time = datetime.now()
    print("Time taken : {}".format(end_time - start_time))
    print(final_df)
    # return final_df.fillna('')
    return final_df


# char_allocation()



# dff['Long Description_r'] = dff.apply(lambda x: x['Long Description'].replace(x['Class'], '').replace(x['Prop_Name'], '').replace(x['Prop_Val'], '').replace(x['uom'], ''), axis = 1)