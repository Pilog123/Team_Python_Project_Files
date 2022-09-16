import pandas as pd
import numpy as np
import re
import time
from datetime import datetime
# from tqdm import tqdm, tqdm_notebook,tqdm_pandas
# from tqdm._tqdm_notebook import tqdm_notebook
# tqdm_notebook.pandas()

# =============================================================================================================

def char_allocation(clause_data,property_cndtn_data,CLASS_data, desc_cls_file, uom_replace_df):

    # start_time = datetime.now()

    # clause_df = pd.read_excel("Clauses-15.03.22.xlsx", 'Sheet2').iloc[:, :5]
    # prop_cndtn_df = pd.read_excel("Is_properties_condition_working.xls", "Sheet4").iloc[:, :9]
    # # sheet5_df = pd.read_excel("Is_properties_condition_working.xls", "Sheet5")
    # desc_cls_file = pd.read_excel('desc_replace.xlsx')
    # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
    # s_qry = """ select {} from {} where SPIR_NO in ('{}') """.format(colsArray, tableName, SPIR_NO)
    # s_qry = """ select {} from {} """.format(colsArray, tableName)

    # print(s_qry)
    # char_data = CallingDatabaseTable(accessName, s_qry)
    # char_data = pd.read_excel('165 Descriptions.xlsx').fillna('')
    # CLASS_data = char_data[:450]
    # # print(type(CLASS_data))
    # print(CLASS_data)
    # file = pd.ExcelFile('ISML UOM_1.xls')
    # UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
    # UOM1 = pd.read_excel(file, 'uom')
    # clauseData = pd.read_excel("Clauses-24.02.22.xlsx", 'Sheet2').iloc[:,:5]
    # property_cndtn_data = pd.read_excel("Is_properties_condition_working_24.02.22.xls").iloc[:,:9]
    # CLASSData = pd.read_excel("spirdataDemo.xls")
    # CLASSData['LONG_DESCRIPTION'] = CLASSData['LONG_DESCRIPTION'].str.upper()
    # CLASSData.drop_duplicates(inplace = True, keep=False, ignore_index=True)
    # CLASSData.columns
    
    # clauseData = pd.read_excel("Clauses-24.02.22.xlsx", 'Sheet2').iloc[:,:5]
    # property_cndtn_data = pd.read_excel("Is_properties_condition_working_24.02.22.xls").iloc[:,:9]
    # CLASSData = pd.read_excel("spirdataDemo.xls")
    # CLASSData['LONG_DESCRIPTION'] = CLASSData['LONG_DESCRIPTION'].str.upper()
    # CLASSData.drop_duplicates(inplace = True, keep=False, ignore_index=True)
    # CLASSData.columns

    try:
    #-------------------------------------------------------------------------------------------------------------------
        start_time = datetime.now()
        clauseData = clause_data
        property_cndtn_data = property_cndtn_data
        CLASSData = CLASS_data
        # CLASSData.rename(columns={'PART_REF_ISSUED': 'RECORD_NO', 'ITEM_DESC': 'LONG_DESCRIPTION', 'OBJ_QUAL': 'CLASS'}, inplace=True)
        # CLASSData.rename(columns={'REGISTERED_RECORD_NO': 'RECORD_NO', 'LONG_TEXT': 'LONG_DESCRIPTION', 'CLASS': 'CLASS'}, inplace=True)

        CLASSData['LONG_DESCRIPTION'] = CLASSData['LONG_DESCRIPTION'].str.upper()
        CLASSData.drop_duplicates(inplace=True, keep='first', ignore_index=True)

        #-------------------------------------------------------------------------------------------------------------------
        # df = pd.read_excel('Is_properties_condition_working_21.03.22_rev1.xls', 'Sheet5')
        # df.drop_duplicates(inplace=True, keep='first', ignore_index=True)
        # print(df)
        #-------------------------------------------------------------------------------------------------------------------

        # CLASSData.columns
        # df = pd.read_excel('Is_properties_condition_working_21.03.22_rev1.xls', 'Sheet5')
        # df = sheet5_df
        # df.drop_duplicates(inplace=True, keep='first', ignore_index=True)
        # print(df)
        #-------------------------------------------------------------------------------------------------------------------
        def cleanDescription(cls, desc_cls_file, des1):
            # -------------------------------------------------------------------------
            if cls in desc_cls_file.CLASS.tolist():

                cls_idx_lst = desc_cls_file.index[desc_cls_file['CLASS'] == cls].tolist()

                for cls_idx in cls_idx_lst:

                    if desc_cls_file['Value'][cls_idx] in des1:
                        print('Orig Desc: ', des1, "|", 'Modified Desc: ', des1.replace(desc_cls_file['Value'][cls_idx], desc_cls_file['Value_replace'][cls_idx]), file=print_file)
                        des1 = des1.replace(desc_cls_file['Value'][cls_idx], desc_cls_file['Value_replace'][cls_idx])
                print('=' * 100, '\n', file=print_file)
            return des1

        # CLASSData.columns
        def altr_des(des):
            des = re.sub(r'\"', 'INCH', des)
            des = re.sub(r' +',' ',des)
            des = re.sub(r'\n',' ',des)
            des = re.sub(r'( ,|, )',', ',des)
            des = re.sub(r'( :|: )',': ',des)
            des = re.sub(r'( ;|; )','; ',des)
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
            des = cleanDescription(cls, desc_cls_file, des)
            prop_name = []

            des1 = des
            des2 = des
            des3 = des

            for pcls,prop,uom,rule_uom,cndtn_chk,ptype in zip(property_cndtn_data['CLASS'],
                                                            property_cndtn_data['PROPERTY'],
                                                            property_cndtn_data['UOM'],
                                                            property_cndtn_data['RULE_UOM'],
                                                            property_cndtn_data['CONDITION_CHECK'],
                                                            property_cndtn_data['TYPE']):

                if cls == pcls and ptype == 'REGEXP':
                    # -------------Cleaning Description----------------
        #             print(prop)
        #             print('condition_check', str(cndtn_chk))
        #             print(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1))
        #             print("=" * 25)
                    # -------------------------------------------------

                    if re.search(r'\b{}\b'.format(cndtn_chk),des1) != None:

                        prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip(),uom,rule_uom])
                        # prop1.append(prop)
                        # prop1.append(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())
                        # prop1.append(uom)
                        # prop1.append(rule_uom)
                        # print(cndtn_chk)
                        des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)

                elif cls == pcls and ptype == 'CLAUSE':

                    # -------------Cleaning Description----------------
        #             des2 = cleanDescription(cls, desc_cls_file, des2)
                    # -------------------------------------------------

                    for cl_name,cl_val,o_des in zip(clauseData['CLAUSE_NAME'],clauseData['CLAUSE_VALUE'], clauseData['O_DESCRIPTOR']):
        #                 if (cndtn_chk == cl_name) and ((pcls == o_des) | (pcls == 'ALL')):
        #                 if (pcls == 'ALL'):
        #                     print(pcls,o_des,cndtn_chk)
                        if (cndtn_chk == cl_name) and (pcls == o_des or o_des == 'ALL'):

                            if cl_val in des2:

                                if re.search(r'\b{}\b'.format(str(cl_val)),des2) != None:
                                    prop_name.append([prop,re.search(r'\b{}\b'.format(str(cl_val)),des2).group().strip(),uom,rule_uom])
                #                             prop1.append(prop)
                #                             prop1.append(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())
                #                             prop1.append(uom)
                #                             prop1.append(rule_uom)
                                    des2 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cl_val)),des2).group().strip())), '', des2)



                elif cls == pcls and ptype == 'NORMAL':
                    # -------------Cleaning Description----------------
        #             des3 = cleanDescription(cls, desc_cls_file, des3)
                    # -------------------------------------------------
                    if re.search(r'\b{}\b'.format(str(cndtn_chk)),des3) != None:
                        prop_name.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des3).group().strip(),uom,rule_uom])
        #                 prop1.append(prop)
        #                 prop1.append(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())
        #                 prop1.append(uom)
        #                 prop1.append(rule_uom)
                        des3 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des3).group().strip())), '', des3)
                else:
                    pass

            prop_list = []
            for i in prop_name:
                if i not in prop_list:
                    prop_list.append(i)

            # print(prop_list)
            # print(des1, ' | ', des)
            # return prop_list, des
            return prop_list, des1

        # =============================================================================================================

        CLASSData['Properties'] = CLASSData.apply(lambda x: property_extraction(x['LONG_DESCRIPTION'],x.CLASS)[0],axis=1)
        CLASSData['des1'] = CLASSData.apply(lambda x: property_extraction(x['LONG_DESCRIPTION'],x.CLASS)[1],axis=1)


        CLASSData.fillna('', inplace = True, axis = 1)

        CLASSData['Prop_Name'] = ''
        CLASSData['Prop_Val'] = ''
        CLASSData['uom'] = ''
        CLASSData['r_uom'] = ''

        CLASSData['Quantity'] = CLASSData['Properties'].apply(lambda x : len(x))
        CLASSData.reset_index(drop=True, inplace=True)

        CLASSData_z = CLASSData[CLASSData['Quantity'] == 0]
        CLASSData_z.reset_index(drop= True, inplace = True)

        CLASSData_n = CLASSData[CLASSData['Quantity'] != 0]
        CLASSData_n.reset_index(drop= True, inplace = True)

        CLASSData_new = CLASSData_n.loc[CLASSData_n.index.repeat(CLASSData_n.Quantity)].reset_index(drop=True)
        CLASSData_new.to_excel('CLASSData_new_Error.xlsx')

        # CLASSData_new = pd.concat([CLASSData_z, CLASSData_1], ignore_index = True)
        # CLASSData_new

        group = CLASSData_new.groupby(['RECORD_NO'])

        CLASSData_new1 = pd.DataFrame(columns=['RECORD_NO', 'CLASS', 'LONG_DESCRIPTION','Properties','Prop_Name','Prop_Val','uom','r_uom',
                                            'Quantity'])

        for i in (CLASSData_new['RECORD_NO'].unique()):

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

            CLASSData_new1 = pd.concat([CLASSData_new1, batch], ignore_index = True)

        # CLASSData_new1.columns
        CLASSData_new1.fillna('', inplace = True, axis = 1)
        # CLASSData_new1
        if len(CLASSData_new1) != 0:

            CLASSData_new1['Prop_Val'] = CLASSData_new1.apply(lambda x: re.sub(r'^[\.\,\:\-\/()+xX]{1,6}|[\.\,\:\-\/()+xX]{1,6}$','',x['Prop_Val']), axis = 1)

            CLASSData_new1['Prop_Val'] = CLASSData_new1.apply(lambda x: re.sub(r'^[\,\:]{1,2}|[\,\:]{1,2}$','',x['Prop_Val']), axis = 1)

            CLASSData_new1['Prop_Val'] = CLASSData_new1['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x)))

            CLASSData_new1['Prop_Val'] = CLASSData_new1.apply(lambda x: x['Prop_Val'].replace(x['uom'], ''), axis = 1)

            CLASSData_new1['Prop_Val'] = CLASSData_new1.apply(lambda x: x['Prop_Val'].replace(x['Prop_Name'], ''), axis = 1)

            CLASSData_new1['Prop_Val'] = CLASSData_new1.apply(lambda x: x['Prop_Val'].replace(x['r_uom'], ''), axis = 1)

            CLASSData_new1['Prop_Val'] = CLASSData_new1.apply(lambda x: re.sub(x['r_uom'], '', x['Prop_Val']),axis=1)

            #     ==============================================================================================================

            group1 = CLASSData_new1.groupby(['RECORD_NO'])

            final_df = pd.DataFrame()
            # CLASSData_new1

            for i in (CLASSData_new1['RECORD_NO'].unique()):
                batch1 = group1.get_group(i)
                batch1['Prop_Val'] = batch1.groupby(['Prop_Name'])['Prop_Val'].transform(lambda x : ' ; '.join(x))
                batch1['uom'] = batch1.groupby(['Prop_Name'])['uom'].transform(lambda x : ','.join(x))
                batch1.drop_duplicates(subset = ['Prop_Name', 'Prop_Val', 'uom'], inplace = True, keep = 'first')
                final_df = pd.concat([final_df, batch1], ignore_index = True)

        #     ==============================================================================================================

            # CLASSData_new1.columns
            final_df.fillna('', inplace = True, axis = 1)

            final_df['Prop_Val'] = final_df.apply(lambda x: re.sub(r'^[\.\,\;\:\-\/()+]{1,3}|[\.\,\:\;\-\/()+]{1,3}$','',x['Prop_Val']), axis = 1)

            final_df['Prop_Val'] = final_df['Prop_Val'].apply(lambda x: re.sub(r'[\[\]\']', '', str(x).strip()))

            final_df['uom'] = final_df['uom'].apply(lambda x: ','.join(list(pd.unique(x.strip().split(',')))))

            final_df['uom'] = final_df.apply(lambda x: re.sub(r'^,|,$','',x['uom']), axis = 1)
            final_df = pd.concat([CLASSData_z, final_df], ignore_index = True)
            final_df.fillna('', inplace = True, axis = 1)
            #     ==============================================================================================================
            # final_df['LONG_DESCRIPTION_EXCLUDE'] = ''
            #
            # for i in range(len(final_df)):
            #     res = ' '.join([ele for ele in final_df['LONG_DESCRIPTION'][i].split(' ') if (ele not in (
            #                 final_df['CLASS'][i].split(',') or final_df['CLASS'][i].split(' ') or final_df['CLASS'][
            #             i].split(', ')))])
            #     #     print(res)
            #     res1 = ' '.join([ele for ele in res.split(' ') if (ele not in (
            #                 final_df['Prop_Name'][i].split(',') or final_df['Prop_Name'][i].split(' ') or
            #                 final_df['Prop_Name'][i].split(', ')))])
            #     #     print(res1)
            #     res2 = ' '.join([ele for ele in res1.split(' ') if (ele not in (
            #                 final_df['Prop_Val'][i].split(',') or final_df['Prop_Val'][i].split(' ') or
            #                 final_df['Prop_Val'][i].split(', ')))])
            #     #     print(res2)
            #     res3 = ' '.join([ele for ele in res2.split(' ') if (ele not in (
            #                 final_df['uom'][i].split(',') or final_df['uom'][i].split(' ') or final_df['uom'][i].split(
            #             ', ')))])
            #     #     print(res3)
            #     res4 = ' '.join([ele for ele in res3.split(' ') if (ele not in (
            #                 final_df['r_uom'][i].split('|') or final_df['r_uom'][i].split(' ') or final_df['r_uom'][
            #             i].split('| ')))])
            #
            #     res5 = ' '.join([ele for ele in res4.split(' ') if
            #                      (ele not in final_df['Prop_Val'][i] + final_df['uom'][i] + final_df['r_uom'][i])])
            #     #     print(res4)
            #
            #     final_df['LONG_DESCRIPTION_EXCLUDE'][i] = res5
            def long_des_exclude(batch):

                des = batch['des1'].unique()
                #         print(des)

                column_values = batch[["CLASS", "Prop_Name", "Prop_Val", "uom"]].values.ravel()
                unique_values = pd.unique(column_values)
                # unique_values.sort(key = len,reverse=True)

                #         print(unique_values)
                q_val = []
                for k in unique_values:
                    q_val.append([q for q in re.split(r'[\s,]', k)])
                q_val = sum(q_val, [])
                q_val.sort(key = len,reverse=True)
                #         print('q_val', q_val, type(q_val))

                #         print('unique_values',unique_values)
                new_des = ' '.join(des)
                #         print('joined des', new_des)
                fft = new_des
                for i in q_val:
                    #         val = re.split(r'[\s,]', i)
                    fft = re.sub(re.escape(i), '', fft)
                # print('fft',fft)
                return fft

            final_df2 = pd.DataFrame()

            group2 = final_df.groupby(['RECORD_NO'])
            for i in (final_df['RECORD_NO'].unique()):
                batch2 = group2.get_group(i)
                batch2.reset_index(drop=True, inplace=True)
                batch2['LONG_DESCRIPTION_EXCLUDE'] = batch2.apply(lambda x: long_des_exclude(batch2), axis=1)
                final_df2 = pd.concat([final_df2, batch2], ignore_index=True)

            final_df2.drop(['Properties', 'Quantity', 'r_uom', 'des1'], axis=1, inplace=True)

        else:
            final_df2 = CLASSData
            final_df2.drop(['Properties', 'Quantity', 'r_uom', 'des1'], axis=1, inplace=True)

    # print(name)
    # print(val)
        final_df2['Prop_Val']= final_df2.apply(lambda x: str(x['Prop_Val']).replace(r';', ','), axis = 1)


        def uom_replace(UOM):
            for trm, uom in zip(uom_replace_df['TERM'], uom_replace_df['UOM']):
                if trm == UOM:
                    #             print(trm, uom, UOM)
                    #             print("===============================================")
                    return uom
            else:
                return UOM


        final_df2['uom'] = final_df2['uom'].apply(lambda x: uom_replace(x))
        final_df2['uom'] = final_df2['uom'].replace('', 'N/A')
        final_df2['LONG_DESCRIPTION_EXCLUDE'] = final_df2.apply(lambda x: re.sub(r'[\,\:\;\"]{1,6}', '', x['LONG_DESCRIPTION_EXCLUDE']),axis=1)
        final_df2['LONG_DESCRIPTION_EXCLUDE'] = final_df2.apply(lambda x: re.sub(r' +',' ', x['LONG_DESCRIPTION_EXCLUDE']), axis=1)
        final_df2['LONG_DESCRIPTION_EXCLUDE'] = final_df2.apply(lambda x: re.sub(r'\"','', x['LONG_DESCRIPTION_EXCLUDE']), axis=1)
        final_df2['LONG_DESCRIPTION_EXCLUDE'] = final_df2.apply(lambda x: re.sub(r'INCH','\"', x['LONG_DESCRIPTION_EXCLUDE']), axis=1)
        final_df2['LONG_DESCRIPTION_EXCLUDE'] = final_df2.apply(lambda x: re.sub(r'HASH','#', x['LONG_DESCRIPTION_EXCLUDE']), axis=1)



        cls_df = property_cndtn_data[property_cndtn_data['TYPE'] == 'CLAUSE']
        cls_df.reset_index(drop=True, inplace=True)
        def prop_val_dup(prop_name1, prop_val1):
            for p_name in cls_df['PROPERTY']:
                if (p_name == prop_name1):
                    prop_val1 = re.sub(r',.*', '', prop_val1)
            return prop_val1

        final_df2['Prop_Val'] = final_df2.apply(lambda x: prop_val_dup(x['Prop_Name'], x['Prop_Val']), axis=1)

        final_df2.to_excel('output_char_extraction_spir_exclude_data_23.03.22.xlsx', index=False)

        end_time = datetime.now()
        print("Time taken : {}".format(end_time - start_time))
        print(final_df2)
        return final_df2.fillna('')

    except Exception as e:
        # print("Exception : ", e)
        return "Please check: "+str(e)
    # return final_df
    # return final_df2


    # char_allocation()




    # dff['LONG_DESCRIPTION_r'] = dff.apply(lambda x: x['LONG_DESCRIPTION'].replace(x['CLASS'], '').replace(x['Prop_Name'], '').replace(x['Prop_Val'], '').replace(x['uom'], ''), axis = 1)