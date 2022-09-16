import pandas as pd
from datetime import datetime
import re

# def char_allocation(clause_data,property_cndtn_data,class_data,UOM,UOM1, desc_cls_file, df):
def char_allocation():

    start_time = datetime.now()
    


    clause_data = pd.read_excel("Clauses-15.03.22.xlsx", 'Sheet2').iloc[:, :5]
    property_cndtn_data = pd.read_excel("Is_properties_condition_working.xls", "Sheet4").iloc[:, :9]
    # class_data = pd.read_excel('STG_ESPIR_25_03_2022.xls', 'Sheet2')
    class_data = pd.read_excel('spir_res(25-03).xlsx')[:10]
    file = pd.ExcelFile('ISML UOM_1.xls')
    UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
    UOM1 = pd.read_excel(file, 'uom')
    desc_cls_file = pd.read_excel('desc_replace.xlsx')
    df = pd.read_excel("Is_properties_condition_working.xls", "Sheet5")

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
        prop_val = []
        prop_uom = []
        prop_r_uom = []


        
        des1 = des
        for pcls,prop,uom,rule_uom,cndtn_chk,ptype in zip(property_cndtn_data['CLASS'],property_cndtn_data['PROPERTY'],property_cndtn_data['UOM'],property_cndtn_data['RULE_UOM'],property_cndtn_data['CONDITION_CHECK'], property_cndtn_data['TYPE']):

            if cls == pcls and ptype == 'REGEXP':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    # prop_lst.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip(),uom,rule_uom])
                    prop_name.append(prop)
                    prop_val.append(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())
                    prop_uom.append(uom)
                    prop_r_uom.append(rule_uom)
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
                                # prop_lst.append([prop,re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip(),uom,rule_uom])
                                prop_name.append(prop)
                                prop_val.append(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())
                                prop_uom.append(uom)
                                prop_r_uom.append(rule_uom)                                
                                des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cl_val)),des1).group().strip())), '', des1)

            elif cls == pcls and ptype == 'NORMAL':
                # -------------Cleaning Description----------------
                des1 = cleanDescription(cls, desc_cls_file, des1)
                # -------------------------------------------------
                if re.search(r'\b{}\b'.format(str(cndtn_chk)),des1) != None:
                    # prop_lst.append([prop,re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip(),uom,rule_uom])
                    prop_name.append(prop)
                    prop_val.append(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())
                    prop_uom.append(uom)
                    prop_r_uom.append(rule_uom)
                    des1 = re.sub(re.escape(str(re.search(r'\b{}\b'.format(str(cndtn_chk)),des1).group().strip())), '', des1)
            else:
                pass
            
        # prop_list = []
        # for i in prop_lst:
        #     if i not in prop_list:
        #         prop_list.append(i)

        # print(prop_list)
        print(des1, ' | ', des)
        # if prop_name, prop_val, prop_uom, prop_r_uom:
        return prop_name, prop_val, prop_uom, prop_r_uom


# =============================================================================================================

    # classData['Properties'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[0],axis=1)

    # classData['p_name'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[0],axis=1)
    # classData['p_val'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[1],axis=1)
    # classData['p_uom'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[2],axis=1)
    # classData['p_r_uom'] = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class)[3],axis=1)

    # pn, pv, po, pro = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class),axis=1)

    # print(pn, '\n', pv, '\n',  po, '\n',  pro)

    # try:
    #     classData_new[['P_NAME', 'P_VAL', 'P_UOM', 'P_RL_UOM']] = [pn, pv, po, pro]
    # except:
    #     classData_new[['P_NAME', 'P_VAL', 'P_UOM', 'P_RL_UOM']] = [None, None, None, None]


    # name = []
    # val = []
    # uom = []
    # r_uom = []

    # for i in range(len(classData)):
    #     for j in range(len(classData['Properties'][i])):
    #         name.append(classData['Properties'][i][j][0])
    #         val.append(classData['Properties'][i][j][1])
    #         uom.append(classData['Properties'][i][j][2])
    #         r_uom.append(classData['Properties'][i][j][3])

   
    classData.fillna('', inplace = True, axis = 1)

    

    classData['Quantity'] = classData['Properties'].apply(lambda x : len(x))

    classData_z = classData[classData['Quantity'] == 0]
    classData_z.reset_index(drop= True, inplace = True)

    classData_n = classData[classData['Quantity'] != 0]
    classData_n.reset_index(drop= True, inplace = True)



    classData_1 = classData_n.loc[classData_n.index.repeat(classData_n.Quantity)].reset_index(drop=True)
    # classData_new

    classData_new = pd.concat([classData_z, classData_1], ignore_index = True)
    

    # # name = []
    # # val = []
    # # uom = []
    # # r_uom = []
    # # for i in range(len(classData)):
    #     # for j in range(len(classData['Properties'][i])):
    #         # name.append(classData['Properties'][i][j][0])
    #         # val.append(classData['Properties'][i][j][1])
    #         # uom.append(classData['Properties'][i][j][2])
    #         # r_uom.append(classData['Properties'][i][j][3])

    # # print(name)
    # # print(val)

    # # print(len(name))
    # # print(len(val))


    # # classData_new['prop_lst'] = name
    # # classData_new['Prop_Val'] = val
    
    classData_new['P_NAME'] = ''
    classData_new['P_VAL'] = ''
    classData_new['P_UOM'] = ''
    classData_new['P_RL_UOM'] = ''

    # print(classData.Properties)
    # print(classData.Properties.tolist())

    
    

    # try:
    #     classData_new[['P_NAME', 'P_VAL', 'P_UOM', 'P_RL_UOM']] = pd.DataFrame(classData.Properties.tolist(), index= classData.index)
    # except:
    #     classData_new[['P_NAME', 'P_VAL', 'P_UOM', 'P_RL_UOM']] = None, None, None, None

    pn, pv, po, pro = classData.apply(lambda x: property_extraction(x['Long Description'],x.Class),axis=1)

    print(pn, '\n', pv, '\n',  po, '\n',  pro)

    try:
        classData_new[['P_NAME', 'P_VAL', 'P_UOM', 'P_RL_UOM']] = [pn, pv, po, pro]
    except:
        classData_new[['P_NAME', 'P_VAL', 'P_UOM', 'P_RL_UOM']] = [None, None, None, None]

    
    
    classData_new.to_excel('classData_new_25.xlsx', index=False)
    return 'Done!'

char_allocation()