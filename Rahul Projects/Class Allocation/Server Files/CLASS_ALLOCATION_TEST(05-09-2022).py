# -*- coding: utf-8 -*-
"""
Created on Thu Sep  1 11:15:09 2022

@author: Keerthi
"""

# import shutil
# import uvicorn
# from fastapi import FastAPI, File, UploadFile
# from fastapi.responses import HTMLResponse
import pandas as pd
import numpy as np
# from fastapi import FastAPI, File, Form, UploadFile
import re
# import threading
import cx_Oracle
from datetime import datetime
# from sqlalchemy import types, create_engine


# app = FastAPI()
# MDRM_V10_TEST:MDRM#vten_tesT@172.16.1.13:1521/?service_name=DP071201

def class_allocation_func(source_DF):
    start_time = datetime.now()
    source_DF = source_DF[source_DF['LONG_TEXT'].notnull()]

    # CLASS TABLE
    check_df = pd.ExcelFile(r'.\CLASS_CLEAN_Item_Details_NIIC.xlsx')
    # check_df = pd.ExcelFile(r'./Item Details/CLASS_CLEAN_Item_Details_NIIC.xlsx')
    class_check = pd.read_excel(check_df,'Object_Qualifier')

    check1 = pd.read_excel(check_df,'Spl')
    comp_cls_df = pd.read_excel(check_df,'Object_Change')

    # DF = pd.read_excel(r'./Item Details/O_RECORD_UNSPSC_CLOUD_22_03_2022.xls')

    alter_df = pd.read_excel(r'.\IS_ALTER_WORD_ABB.xlsx')
    # alter_df = pd.read_excel('IS_ALTER_WORD_ABB.xlsx')
    alter_df.drop(alter_df[alter_df['ALTER_WORD'].isnull()].index,inplace=True)
    alter_df.drop(alter_df[alter_df['WORD'].isnull()].index,inplace=True)

    ### Exclusions ###
    exmp = pd.ExcelFile(r'.\Exclusions.xlsx')
    # exmp = pd.ExcelFile(r'./Item Details/Exclusions.xlsx')
    exmp_df = pd.read_excel(exmp,'Sheet2')
    exmp_cls_df = pd.read_excel(exmp,'Sheet1')
    exmp_lst_df = pd.read_excel(exmp,'Sheet3')
    dict_df = pd.read_excel(check_df,'dict_chk')

    r1_check = pd.read_excel(check_df,'Rule-1')
    obj_check = class_check[class_check['TERM(Qualifier)'].notnull()]
    lst_check = pd.read_excel(check_df,'Last')
    pattern = r'(\bFOR\b|F/|W/|\bWITH\b|\bAT\b|\bINCLUDE\b|\bINCLUDING\b|\bAND\b|\bCONSISTS\b|\bINCLUDES\b|&|\bWITHOUT\b|\bWITH OUT\b|W/O)'

    

    # ALTER WORD REPLACEMENT
    def altr_word(des):
        des = des.upper()
        des = re.sub('(;|:)',',',des)
        des = re.sub(r'[^A-Za-z0-9\s\.\/,&:\(\)\'\-\"]',' ',des)
        des = re.sub(r'\n',' ',des)
        des = re.sub(r'("O"RING|"O" RING)','O-RING',des)
        des = re.sub(r'\"[,]','\" ,',des)
        des = re.sub(r'(\")\W',' INCH ',des)
        if re.search(r'[0-9]IN\b',des):
            des = re.sub(r'IN\b',' IN',des)
        des = re.sub(r'\b(\"|IN)\b',' INCH ',des)
        x = re.sub(r' +',' ',des)
        
        for word,altr in zip(alter_df['WORD'],alter_df['ALTER_WORD']):
            if (altr in x) & (len(altr)>2) :
                altr = r'\b{}\b'.format(altr)
                x = re.sub(altr,word,x)
                x = re.sub(r',+',',',x)
        return x

    def exclusion_cndtn(des1):
        des = des1.split(',')[0]
        for exmp_obj,trm in zip(exmp_df['Object'],exmp_df['Exemptions']):
            for exmp_trm in trm.split('|'):
                try:
                    if ((re.search(r'\b{}\b'.format(exmp_obj), 
                                des)) and (re.search(r'\b[^-]{}\b'.format(exmp_trm), 
                                                    des))) or ((re.search(r'\b{}\b'.format(exmp_obj), 
                                                                        des)) and (re.search(r'\b{}\b'.format(exmp_trm), des))):

                        if des.index(exmp_obj) < des.index(re.search(r'\b{}\b'.format(exmp_trm), des).group()):
                            des1 = re.sub(re.search(r'\b{}\b'.format(exmp_trm), des).group(),'',des1)
                except:
                    des1 = des1
        return des1

    def multi_cls_cndtn(des,cls,st_des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        pat = r'(,|\bFOR\b|F/|W/|\bWITH\b|\bAT\b|\bINCLUDE\b|\bINCLUDING\b|\bAND\b|\bCONSISTS\b|\bINCLUDES\b|\bWITHOUT\b|\bW/O\b|&|\bWITH OUT\b)'
        lst_ob = []
        lst_qu = []
        # des1 = des
        if '|' in st_des:
            st = st_des.split('|')
            for s in st[0].split():
                if re.search(r'\b{}\b'.format(s), re.sub(r'{}.*'.format(pat),'',des)):  
                    # des1 = re.sub(r'{}'.format(s),'',des1)
                    # des2 = des1
                    lst_ob.append(s)
                else:
                    lst_ob.append('')
            for i in st[1:]:
                if re.search(r'\b({})\b'.format(i),des):
                    # des3 = re.sub(r'{}'.format(i),'',des1)
                    # des2 = des3
                    lst_qu.append(i)
            if ((lst_ob != []) & ('' not in lst_ob)) & (lst_qu != []):
                return cls
            else:
                return None
        else:
            st = st_des.split()
            for s in st:
                if re.search(r'\b{}\b'.format(s), re.sub(r'{}.*'.format(pat),'',des)):
                    # des1 = re.sub(r'{}'.format(s),'',des1)
                    # des2 = des1
                    lst_ob.append(s)
                else:
                    lst_ob.append('')
            if ((lst_ob != []) & ('' not in lst_ob)):
                return cls
            else:
                return None

    def multi_cls(des):
        for cls, st_des in zip(comp_cls_df['Class'], comp_cls_df['OBJ_QUAL']):
            if multi_cls_cndtn(des,cls,st_des):
                return multi_cls_cndtn(des,cls,st_des)



    def rule1_cndtn(des):
        r1_cls = []
        ltr_lst = []
        
        ltr_lst = set([s[0] for s in des.split()])
        r1_fltrd = pd.DataFrame()
        for i in ltr_lst:
            r1_samp = r1_check.loc[r1_check['RULE1'].str.startswith(i, na=False)]
            r1_fltrd = pd.concat([r1_fltrd,r1_samp])
            for cls, r1 in zip(r1_fltrd['Class'],r1_fltrd['RULE1']):
                if re.search(r'\b({})\b'.format(r1),des):
                    r1_cls.append([cls, re.search(r'\b({})\b'.format(r1),des).group()])
   
        r1_list = []
        for i in r1_cls:
            if i not in r1_list:
                r1_list.append(i)
        r1_str_list = [i[1] for i in r1_list]
        r1_cls_list = [i[0] for i in r1_list]
        if r1_str_list != []:
            r1_ind = [des.index(i) for i in r1_str_list]

            return [r1_cls_list[r1_ind.index(max(r1_ind))], r1_str_list[r1_ind.index(max(r1_ind))]]
        

    def rule1(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +',' ',des)
        
        pat = r'{}.*'.format(pattern)
        mat = re.search(pattern,des)
        if mat:
            x = re.sub(pat,'',des)  
            return rule1_cndtn(x.split(',')[0])
            
        else:
            return rule1_cndtn(des.split(',')[0])
                
            
    def obj_qual_cndtn(x): 
        objct = []
        x_lst = [i[:1] for i in list(set([re.sub('\W','','{}'.format(i)) for i in x.split()]))]
        cl_ck_df = pd.concat([dict_df['{}'.format(i)] for i in x_lst])
        for obj in cl_ck_df:
            if re.search(r'\b({})\b'.format(obj),x):
                if '|' in obj:
                    objct.append(re.search(r'\b({})\b'.format(obj),x).group())
                else:
                    objct.append(obj)
        obj_list = []
        for i in objct:
            if i not in obj_list:
                obj_list.append(i)
    #     print(obj_list)
    
        x = re.sub(r'[^A-Za-z0-9\s:\'\-\/\.]',' ',x)
        x = re.sub(r' +',' ',x)
        if len(obj_list)==1:
            return obj_list[0]
        elif len(obj_list)>1:
    #         ob_lst,splt = exclusion_cndtn(x,obj_list,splt)
    #         print(ob_lst)
            def string_set(string_list):
                return set(i for i in string_list 
                           if not any(i in s for s in string_list if (i != s and ((' ' in s) or ('-' in s)))))
            ob_str = [re.sub(r'\^','',i) for i in obj_list]
            obj_str = list(string_set(ob_str))
            
    #         print(obj_str)
            
            if obj_str != []:
    #             print(x,obj_str)
                indx = [x.rindex(i) for i in obj_str]
                return obj_str[indx.index(max(indx))]
        else:
            return None
    # def obj_qual_cndtn(x): 
    #     objct = []
    #     for obj in class_check['TERM(OBJECT)']:
    #         if re.search(r'\b({})\b'.format(obj),x):
    #             if '|' in obj:
    #                 objct.append(re.search(r'\b({})\b'.format(obj),x).group())
    #             else:
    #                 objct.append(obj)
    #     obj_list = []
    #     for i in objct:
    #         if i not in obj_list:
    #             obj_list.append(i)

    #     x = re.sub(r'[^A-Za-z0-9\s:\'\-\/\.]',' ',x)
    #     x = re.sub(r' +',' ',x)
    #     if len(obj_list)==1:
    #         return obj_list[0]
    #     elif len(obj_list)>1:
    #         splt = x.split(' ')
    #         def string_set(string_list):
    #             return set(i for i in string_list 
    #                     if not any(i in s for s in string_list if (i != s and ((' ' in s) or ('-' in s)))))
    #         ob_str = [re.sub(r'\^','',i) for i in obj_list]
    #         obj_str = list(string_set(ob_str))
            
    #         if obj_str != []:
    #             indx = [x.rindex(i) for i in obj_str]
    #             return obj_str[indx.index(max(indx))]
    #     else:
    #         return None

    def object(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +',' ',des)
        
        # split description with (FOR, WITH, F/, W/)
        pat = r'{}.*'.format(pattern)
        mat = re.search(pattern,des)
        if mat:
            # des before split terms
            x = re.sub(pat,'',des)  
            # des before ',' 
            return obj_qual_cndtn(x.split(',')[0])
        
        # same procedure with des without (FOR,WITH,F/,W/)
        else:
            return obj_qual_cndtn(des.split(',')[0])

    def object2(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +',' ',des)
        
        # split description with (FOR, WITH, F/, W/)
        pat = r'{}.*'.format(pattern)
        mat = re.search(pattern,des)
        if mat:
            # des before split terms
            x = re.sub(pat,'',des) 
            # des before second ',' 
            if ',' in x:
                return obj_qual_cndtn(x.split(',')[1])
            
        else:
            # des before second ','
            if ',' in des:
                return obj_qual_cndtn(des.split(',')[1])

    def obj_actual(ob):
        for cls,term in zip(class_check['Class'],class_check['TERM(OBJECT)']):
            trm = re.split(r'\|',term)
            if '^' in ob:
                return re.sub(r'\^','',ob)
            elif (ob in trm) and (ob == cls):
                return term
            
    def obj_synonym(ob):
        for term in class_check['TERM(OBJECT)']:
            trm = re.split(r'\|',term)
            if ('^' in term) and (r'^{}'.format(ob) in term):
                return term
            elif ob in trm:
                return term
            
    def fnl_obj(ob1,ob2):
        if ob1==ob2:
            return ob1
        elif ob1 is None:
            return ob2
        elif ob2 is None:
            return ob1
        elif re.search(ob1,ob2):
            return re.search(ob1,ob2).group()
        elif ob1 is None and ob2 is None:
            return None

    def obj_qualifier(x,obj):
        qual1 = []
        qual1_str = []
        if ' ' in obj:
            chk = obj_check[obj_check['TERM(OBJECT)'].isin([i for i in obj_check['TERM(OBJECT)'] if (re.search(r'\b({})\b'.format(obj),i)) and (re.search(r'(\s)',i))]) & obj_check['TERM(Qualifier)'].notnull()]
            for cls, ob, qual in zip(chk['Class'],chk['TERM(OBJECT)'],chk['TERM(Qualifier)']):
                if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*','',r'{}\b'.format(obj)),qual),cls) or re.search(r'\b({} {})\b'.format(re.sub(r'(\|).*','',r'{}\b'.format(obj)),qual),cls):
                    if re.search(r'\b({})\b'.format(qual),x):
                        qual1.append(qual)
                        qual1_str.append(re.search(r'\b({})\b'.format(qual),x).group())
            if qual1_str != []:
                qual1_ind = [x.index(i) for i in qual1_str]
                return qual1[qual1_ind.index(min(qual1_ind))]
        else:
            chk = obj_check[obj_check['TERM(OBJECT)'].isin([i for i in obj_check['TERM(OBJECT)'] if (re.search(r'\b({})\b'.format(obj),i)) and (not re.search(r'(\s)',i))]) & obj_check['TERM(Qualifier)'].notnull()]
            for cls, ob, qual in zip(chk['Class'],chk['TERM(OBJECT)'],chk['TERM(Qualifier)']):
                if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*','',r'{}\b'.format(obj)),qual),cls) or re.search(r'\b({} {})\b'.format(re.sub(r'(\|).*','',r'{}\b'.format(obj)),qual),cls):
                    if re.search(r'\b({})\b'.format(qual),x):
                        qual1.append(qual)
                        qual1_str.append(re.search(r'\b({})\b'.format(qual),x).group())
            if qual1_str != []:
                qual1_ind = [x.index(i) for i in qual1_str]
                return qual1[qual1_ind.index(min(qual1_ind))]


    def fnl_cls(des,ob,qu):
        for cls,obj,qua in zip(class_check['Class'],class_check['TERM(OBJECT)'],class_check['TERM(Qualifier)']):
            # des1 = des
            # des1 = re.sub(r'\b{}\b'.format(ob),'',des1)
            
            if (qu!= ''):#((qu != None) or (qu != np.NaN)) or (qu!= ''):
                # des1 = re.sub(r'\b{}\b'.format(re.search(r'\b{}\b'.format(qu),des).group()),'',des1)
                if ',' in cls:
                    if (re.search(r'\b{}\b'.format(str(ob)),re.sub(r'(,).*','',cls))) and (re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)','',cls)),qu)):
                        if ', '.join([re.search(ob,re.sub(r'(,).*','',cls)).group(), re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)','',cls)),qu).group()]) == cls:
                            return cls
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*','',r'{}\b'.format(ob))
                            else:
                                return ob
                else:
                    if (re.search(r'\b{}\b'.format(str(ob)),re.sub(r'(\s).*','',cls))) and (re.search(r'\b{}\b'.format(re.sub(r'.*(\s)','',cls)),qu)):
                        if ' '.join([re.search(ob,re.sub(r'(\s).*','',cls)).group(), re.search(r'\b{}\b'.format(re.sub(r'.*(\s)','',cls)),qu).group()]) == cls:
                            return cls
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*','',r'{}\b'.format(ob))
                            else:
                                return ob
            else:
                if (ob == cls):
                    return ob
                elif '|' in ob:
                    if (ob == obj) and (re.sub(r'(\|).*','',r'{}\b'.format(ob)) == cls):
                        return re.sub(r'(\|).*','',r'{}\b'.format(ob))
        

    def frst_cls(des,cls,st_des):
        pat = r'(\bFOR\b|F/|W/|\bWITH\b|\bAT\b|\bINCLUDE\b|\bINCLUDING\b|\bAND\b|\bCONSISTS\b|\bINCLUDES\b|,|\bWITHOUT\b|\bW/O\b|&|\bWITH OUT\b)'
        lst=[]
        st = st_des.split()
        # des1 = des
        if re.search(r'\b{}\b'.format(st[0]), re.sub(r'{}.*'.format(pat),'',des)):
            lst.append(st[0])
            for i in st[1:]:
                if re.search(r'\b{}\b'.format(i),des):
                    lst.append(i)
                else:
                    lst.append('')
        if ('' not in lst) & (lst != []):
            # des1 = re.sub(r'{}'.format(st[0]),'',des1)
            # des2 = des1
            # for i in st[1:]:
                # des3 = re.sub(r'{}'.format(i),'',des2)
                # des2 = des3
            return cls
        else:
            return None
    def frst_chk(des):  
        for chk_des,cls in zip(check1['DESCRIPTION'],check1['CLASS']):
            if frst_cls(des,cls,chk_des):
                return frst_cls(des,cls,chk_des)

    def last_cls(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +',' ',des)
        # des1 = des
        for lst_cls, lst_obj in zip(lst_check['Class'],lst_check['TERM(OBJECT)']):
            pat = r'{}.*'.format(pattern)
            mat = re.search(pattern,des)
            if mat:
                b = re.sub(pat,'',des)
                b = b.split(',')[0]
                if re.search(r'\b({})\b'.format(lst_obj),b):
                    # des1 = re.sub(r'{}'.format(re.search(r'\b({})\b'.format(lst_obj),b).group()),'',des1)
                    return lst_cls
            else:
                if re.search(r'\b({})\b'.format(lst_obj),des):
                    # des1 = re.sub(r'{}'.format(re.search(r'\b({})\b'.format(lst_obj),des1).group()),'',des1)
                    return lst_cls

    def final_class(des,des2,r1,r1_cls,ob,obj_cls,qual):
        des2 = re.sub(r'\([^()]*\)', ' ', des2)
        des2 = re.sub(r' +',' ',des2)
        # des1 = des
        if (r1_cls is not np.NaN) and (obj_cls is not np.NaN):
            ob = re.sub(r'\^','',ob)
            if re.search(r'({})'.format(obj_cls),r1_cls):
                # des1 = re.sub(r'{}'.format(r1),'',des1)
                return r1_cls
            elif (des2.index(r1) < des2.index(ob)) or ((re.search('({})'.format(r1),ob) or (re.search('({})'.format(r1),obj_cls))) or re.search(r'({})'.format(ob),r1)):
                # des1 = re.sub(r'{}'.format(ob),'',des1)
                # if qual is not np.NaN:
                    # des1 = re.sub(r'\b{}\b'.format(re.search(r'\b{}\b'.format(qual),des).group()),'',des1)
                return obj_cls
            else:
                # des1 = re.sub(r'{}'.format(r1),'',des1)
                return r1_cls
        elif (r1_cls is not np.NaN) and (obj_cls is np.NaN):
            # des1 = re.sub(r'{}'.format(r1),'',des1)
            return r1_cls
        elif (r1_cls is np.NaN) and (obj_cls is not np.NaN):
            ob = re.sub(r'\^','',ob)
            # des1 = re.sub(r'{}'.format(ob),'',des1)
            # if qual is not np.NaN:
                # des1 = re.sub(r'\b{}\b'.format(re.search(r'\b{}\b'.format(qual),des).group()),'',des1)
            return obj_cls
        else:
            return np.NaN


    ###### Replacing alter words using altr_word function and creating New+_description #######
    # source_DF['New_Source_Long_Description'] = source_DF['SHORT_DESCRIPTION'].apply(lambda des:altr_word(des))
    print(source_DF)
    source_DF['Alter_Source_Long_Description'] = source_DF['LONG_TEXT'].apply(lambda des:altr_word(str(des)))

    source_DF['New_Source_Long_Description'] = source_DF['Alter_Source_Long_Description'].apply(lambda des:exclusion_cndtn(des))

    source_DF['First_CLASS'] = source_DF['New_Source_Long_Description'].apply(lambda des:multi_cls(des))


    # source_DF['First_CLASS'] = source_DF[source_DF['First_CLASS_'].notnull()]['First_CLASS_'].apply(lambda x:x[0] if x is not np.NaN else x)

    # source_DF['DESCRIPTION_FFT0'] = source_DF[source_DF['First_CLASS_'].notnull()]['First_CLASS_'].apply(lambda x:x[1] if x is not np.NaN else x)


    try:
        source_DF['RULE1'] = source_DF[source_DF['First_CLASS'].isnull()]['New_Source_Long_Description'].apply(lambda des:rule1(des))
    except:
        source_DF['RULE1'] = np.NaN

    df1 = source_DF.loc[pd.isnull(source_DF.RULE1)]
    df2 = source_DF.loc[pd.notnull(source_DF.RULE1)]
    try:
        df2[['CLASS_TYPE1','RULE1_STRING']] = pd.DataFrame(df2.RULE1.tolist(), index= df2.index)
    except:
        df2[['CLASS_TYPE1','RULE1_STRING']] = np.NaN, np.NaN
    source_DF = pd.concat([df1,df2],ignore_index=True)


    source_DF['Object_'] = source_DF[source_DF['First_CLASS'].isnull()]['New_Source_Long_Description'].apply(lambda des:object(des))



    source_DF['Object1'] = source_DF[source_DF['Object_'].notnull()]['Object_'].apply(lambda x:obj_synonym(x))
    source_DF['Object2'] = source_DF[source_DF['Object_'].notnull()]['Object_'].apply(lambda x:obj_actual(x))

  
    try:
        source_DF['Object'] = source_DF[source_DF['Object_'].notnull()].apply(lambda x:fnl_obj(x.Object1,x.Object2),axis=1)
    except:
        source_DF['Object'] = np.NaN


    try:
        source_DF['Obj_Qualifier'] = source_DF[source_DF['Object'].notnull()].apply(lambda x:obj_qualifier(x.Alter_Source_Long_Description, 
                                                                                                x.Object),axis=1)

    except:
        source_DF['Obj_Qualifier'] = np.NaN
    source_DF.replace([None],np.NaN,inplace=True)
    
    # source_DF.replace(np.NaN,'',inplace=True)
    source_DF.replace({'Object':{np.NaN:''}, 'Obj_Qualifier':{np.NaN:''}},inplace = True)

    try:
        source_DF['CLASS2'] = source_DF[source_DF['Object'].notnull()].apply(lambda x: fnl_cls(x.Alter_Source_Long_Description,x.Object, 
                                                                            x.Obj_Qualifier), axis=1)
    except:
        source_DF['CLASS2'] = np.NaN


    source_DF.replace([None],np.NaN,inplace=True)

    # source_DF['CLASS2'] = source_DF['CLASS2'].apply(lambda x:x[0] if x is not np.NaN else x)
  
    # source_DF.replace('',np.NaN,inplace=True)

    source_DF.replace({'RULE1_STRING':{np.NaN:''}, 'CLASS_TYPE1':{np.NaN:''}, 'Object_':{np.NaN:''}, 'CLASS2':{np.NaN:''}},inplace = True)

    source_DF['FINAL_CLASS'] = source_DF[source_DF['First_CLASS'].isnull()].apply(lambda x: final_class(x.Alter_Source_Long_Description,
                                                x.New_Source_Long_Description, x.RULE1_STRING, x.CLASS_TYPE1, x.Object_, x.CLASS2,x.Obj_Qualifier),axis=1)

    # source_DF['FINAL_CLASS'] = source_DF[source_DF['FINAL_CLASS_'].notnull()]['FINAL_CLASS_'].apply(lambda x:x[0] if x is not np.NaN else x)
    # source_DF['DESCRIPTION_FFT1'] = source_DF[source_DF['FINAL_CLASS_'].notnull()]['FINAL_CLASS_'].apply(lambda x:x[1] if x is not np.NaN else x)

    source_DF.fillna('',inplace=True)
    source_DF.replace('',np.NaN,inplace=True)
  
    print(source_DF)

    source_DF['CLASS_TYPE0'] = source_DF[source_DF['FINAL_CLASS'].isnull() & 
                        source_DF['First_CLASS'].isnull()]['Alter_Source_Long_Description'].apply(lambda des:frst_chk(des))

    

    # source_DF.replace([None],np.NaN,inplace=True)

    # source_DF['CLASS_TYPE0'] = source_DF[source_DF['CLASS_TYPE0_'].notnull()]['CLASS_TYPE0_'].apply(lambda x:x[0] if x is not np.NaN else x)

    # source_DF['DESCRIPTION_FFT2'] = source_DF[source_DF['CLASS_TYPE0_'].notnull()]['CLASS_TYPE0_'].apply(lambda x:x[1] if x is not np.NaN else x)


    # source_DF.replace([None],np.NaN,inplace=True)

    
    source_DF['Object_2'] = source_DF[source_DF['CLASS_TYPE0'].isnull() & source_DF['FINAL_CLASS'].isnull() & source_DF['First_CLASS'].isnull()
                        ]['New_Source_Long_Description'].apply(lambda des:object2(des))

    source_DF['Object21'] = source_DF[source_DF['Object_2'].notnull()]['Object_2'].apply(lambda x:obj_synonym(x))
    source_DF['Object22'] = source_DF[source_DF['Object_2'].notnull()]['Object_2'].apply(lambda x:obj_actual(x))


    try:
        source_DF['Object2_'] = source_DF[source_DF['Object_2'].notnull()].apply(lambda x:fnl_obj(x.Object21,x.Object22),axis=1)
    except:
        source_DF['Object2_'] = np.NaN

    try:
        source_DF['Obj_Qualifier2'] = source_DF[source_DF['Object2_'].notnull()].apply(lambda x:obj_qualifier(x.Alter_Source_Long_Description, 
                                                                                                x.Object2_),axis=1)
    except:
        source_DF['Obj_Qualifier2'] = np.NaN



    source_DF.replace(np.NaN,'',inplace=True)
  
    try:
        source_DF['CLASS21'] = source_DF[source_DF['Object2_']!=''].apply(lambda x: fnl_cls(x.Alter_Source_Long_Description,
                                                                                    x.Object2_, x.Obj_Qualifier2), axis=1)
        
    except:
        source_DF['CLASS21'] = np.NaN


    # source_DF['CLASS21'] = source_DF[source_DF['CLASS2_'].notnull()]['CLASS2_'].apply(lambda x:x[0] if x is not np.NaN else x)
    # source_DF['DESCRIPTION_FFT4'] = source_DF[source_DF['CLASS2_'].notnull()]['CLASS2_'].apply(lambda x:x[1] if x is not np.NaN else x)

    source_DF.replace('',np.NaN,inplace=True)

    source_DF['CLASS_LAST'] = source_DF[source_DF['FINAL_CLASS'].isnull() & source_DF['First_CLASS'].isnull() & source_DF['CLASS21'].isnull() &
                        source_DF['CLASS_TYPE0'].isnull()]['Alter_Source_Long_Description'].apply(lambda des:last_cls(des))

    # source_DF['CLASS_LAST'] = source_DF[source_DF['CLASS_LAST_'].notnull()]['CLASS_LAST_'].apply(lambda x:x[0] if x is not np.NaN else x)

    # source_DF['DESCRIPTION_FFT3'] = source_DF[source_DF['CLASS_LAST_'].notnull()]['CLASS_LAST_'].apply(lambda x:x[1] if x is not np.NaN else x)



    source_DF['NEW_CLASS'] = source_DF[['First_CLASS','CLASS_TYPE0','FINAL_CLASS','CLASS_LAST',
                        'CLASS21']].apply(lambda x : '{}{}{}{}{}'.format(x[0],x[1],x[2],x[3],x[4]), axis=1)

    source_DF['NEW_CLASS'] = [re.sub(r'(None|nan)','',x) for x in source_DF['NEW_CLASS']]

    cross_chk_Df = source_DF[source_DF['NEW_CLASS'].notnull()]
    total_class_df = pd.concat([class_check,lst_check,comp_cls_df],ignore_index=True)
    cross_chk_Df['NEW_CLASS'] = cross_chk_Df['NEW_CLASS'].apply(lambda x: 'No Class' if x in cross_chk_Df[cross_chk_Df['NEW_CLASS'].isin(total_class_df['Class']) == False]['NEW_CLASS'].tolist() else x)
    source_DF.update(cross_chk_Df)
    source_DF.replace('No Class',np.NaN,inplace=True)


    # source_DF['DESCRIPTION_FFT'] = source_DF[['DESCRIPTION_FFT0','DESCRIPTION_FFT1','DESCRIPTION_FFT2','DESCRIPTION_FFT3',
    #                         'DESCRIPTION_FFT4']].apply(lambda x : '{}{}{}{}{}'.format(x[0],x[1],x[2],x[3],x[4]), axis=1)

    # source_DF['DESCRIPTION_FFT'] = [re.sub(r'(None|nan)','',x) for x in source_DF['DESCRIPTION_FFT']]


    source_DF.drop(['Alter_Source_Long_Description', 'New_Source_Long_Description',
         'First_CLASS', 'RULE1',
        'CLASS_TYPE1', 'RULE1_STRING', 'Object_', 'Object1', 'Object2',
        'Object', 'Obj_Qualifier', 'CLASS2', 'FINAL_CLASS',
         'CLASS_TYPE0','CLASS_LAST', 'Object_2',
        'Object21', 'Object22', 'Object2_', 'Obj_Qualifier2', 'CLASS21',
            ],axis=1,inplace=True)

    source_DF.replace('O-RING','O RING',inplace=True)
    source_DF.replace('VALVE, MANIFOLD', 'MANIFOLD, VALVE', inplace=True)

    source_DF.fillna('',inplace=True)
    print(source_DF)
    end_time = datetime.now()
    print("Time taken : {}".format(end_time - start_time))

    return source_DF

#########################################################################################################################################################################################################################################
def unspsc_allocation_func(source_dataframe):   
    ######### If using UNSPSC Codes Revised-v1.0.xlsx file for UNSPSC allocation, Use below functions ##########
    # unspsc = pd.read_excel('UNSPSC Codes Revised-v1.0.xlsx')
    
    # def code_unspsc(cls):
    #     for trm,code in zip(unspsc['PPO Class'],unspsc['UNSPSC Code (Class)']):
    #         if trm.upper() == cls:
    #             return code
    # def desc_unspsc(cls):
    #     for trm,desc in zip(unspsc['PPO Class'],unspsc['UNSPSC Description (Class)']):
    #         if trm.upper() == cls:
    #             return desc
    # def unspsc_seg(cls):
    #     for trm,code,desc in zip(unspsc['PPO Class'],unspsc['UNSPSC Code (Segment)'],unspsc['UNSPSC Description (Segment)']):
    #         if trm == cls:
    #             return str(code)+': '+desc
    # def unspsc_family(cls):
    #     for trm,code,desc in zip(unspsc['PPO Class'],unspsc['UNSPSC Code (Family)'],unspsc['UNSPSC Description (Family)']):
    #         if trm == cls:
    #             return str(code)+': '+desc
            
    ########## If UNSPSC table from database(ONTG_Classification) #############
    # conn = cx_Oracle.connect('DR1024193/Pipl#mdrm$93@172.16.1.61:1521/DR101412')
    conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
    unspsc =  pd.read_sql('''
                            SELECT * FROM ONTG_CLASSIFICATION
                        ''', conn)
    conn.close()

    unspsc.fillna('',inplace=True)

    def code_unspsc(cls):
        for trm, code in zip(unspsc['TERM'], unspsc['UNSPSC_CODE']):
            if trm == cls:  # If obtained class is same as table class then return respective table unspsc code
                return code

    def desc_unspsc(cls):
        for trm, desc in zip(unspsc['TERM'], unspsc['UNSPSC_DESC']):
            if trm == cls:  # If obtained class is same as table class then return respective table unspsc description
                return desc

    def unspsc_seg(cls):
        for trm, code, desc in zip(unspsc['TERM'], unspsc['UNSPSC_CODE_SEGMENT_LEVEL'],
                                   unspsc['UNSPSC_DESC_SEGMENT_LEVEL']):
            if trm == cls:  # If obtained class is same as table class then return combination of respective table segement code and des
                return code + ': ' + desc

    def unspsc_family(cls):
        for trm, code, desc in zip(unspsc['TERM'], unspsc['UNSPSC_CODE_FAMILY_LEVEL'],
                                   unspsc['UNSPSC_DESC_FAMILY_LEVEL']):
            if trm == cls:  # If obtained class is same as table class then return combination of respective table family level code and des
                return code + ': ' + desc

    def unspsc_class(cls):
        for trm, code, desc in zip(unspsc['TERM'], unspsc['UNSPSC_CODE_CLASS_LEVEL'],
                                   unspsc['UNSPSC_DESC_CLASS_LEVEL']):
            try:
                if trm == cls:
                    return code + ': ' + desc
            except:
                # print('Error In ------> {}'.format(ERROR_value))
                return ''

    def unspsc_ISIC_class(cls):
        for trm, code, desc in zip(unspsc['TERM'], unspsc['ISIC_CODE'], unspsc['ISIC_CODE_DESC']):
            try:
                if trm == cls:
                    return code + ': ' + desc
            except:
                # print('Error In ------> {}'.format(ERROR_value))
                return ''

    def unspsc_HSN_class(cls):
        for trm, code, desc in zip(unspsc['TERM'], unspsc['HSN_HTS_CODE'], unspsc['HSN_HTS_DESC']):
            try:
                if trm == cls:
                    return code + ': ' + desc
            except:
                # print('Error In ------> {}'.format(ERROR_value))
                return ''
    
    
    source_dataframe['UNSPSC_CODE_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: code_unspsc(cls))
    source_dataframe['UNSPSC_DESCRIPTION_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: desc_unspsc(cls))
    source_dataframe['UNSPSC_SEGMENT_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: unspsc_seg(cls))
    source_dataframe['UNSPSC_FAMILY_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: unspsc_family(cls))
    source_dataframe['UNSPSC_CLASS_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: unspsc_class(cls))  # @# NEw

    source_dataframe['ISIC_CLASS_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: unspsc_ISIC_class(cls))  # @# NEw
    source_dataframe['HSN_CLASS_DATA'] = source_dataframe['NEW_CLASS'].apply(lambda cls: unspsc_HSN_class(cls))  # @# NEw
    
    source_dataframe.fillna('',inplace=True)

    return source_dataframe


#---------------------------------   End Of Code  --------------------------------------------------------------------------------------------
# d  = pd.ExcelFile(r'Medical data.xlsx')
# source_DF = pd.read_excel(r'.\Product Template 1000.xlsx')
# source_DF.rename(columns={'Description':'LONG_TEXT'}, inplace=True)
# df = class_allocation_func(source_DF)
# df[df['NEW_CLASS'] == '']
# print(df.head(50))
# print(df.tail(50))
# df.to_excel('CLASS_ALLOCATION_RES.xlsx',index=False)


#################################################################################################################################################################################################################################
# @app.post("/class_allocation/")
# async def class_allocation(analysisType: str = Form(...), tableName: str = Form(...), colsArray: str = Form(...), accessName: str = Form(...)):

#     def allocation(tableName, colsArray, accessName): 
#         con = cx_Oracle.connect('{}'.format(accessName))      
#         print('SELECT {} FROM {}'.format(colsArray, tableName))
#         input_df = pd.read_sql('SELECT {} FROM {}'.format(colsArray, tableName) ,con)
#         con.close()
#         return input_df
            
#     def connection(analysisType):
#         if analysisType == 'CLASS_Allocation':
#             source_DF = allocation(tableName, colsArray, accessName)
#             return class_allocation_func(source_DF) 
        
#         elif analysisType == 'UNSPSC_Allocation':
#             source_DF = allocation(tableName, colsArray, accessName)
#             source_DF['LONG_TEXT'] = source_DF['LONG_TEXT'].fillna(value=source_DF['SHORT_TEXT'])
#             class_allocation_df = class_allocation_func(source_DF)
#             return unspsc_allocation_func(class_allocation_df)
    
#     #print(DF)
#     return {
#         "RESULT":  tableName
#     }
# @app.get("/")
# async def main():
# #     content = """
# # <body>
# # <form action="/uploadfiles/" enctype="multipart/form-data" method="post">
# # <input name="file" type="file" multiple>
# # <input type="submit">
# # </form>
# # </body>
# #     """
#     return #HTMLResponse(content=content)

# if __name__ == '__main__':
#     uvicorn.run("NEW_CLASS_AND_UNSPSC allocation(API) 22-03:app", port= 5555, reload=True, access_log=False)