
import pandas as pd
import numpy as np
import re
import sys


# MDRM_V10_TEST:MDRM#vten_tesT@172.16.1.13:1521/?service_name=DP071201

def class_allocation_func(source_DF):
    alter_df = pd.read_excel('IS_ALTER_WORD_ABB_FINAL.xlsx')
    alter_df.drop(alter_df[alter_df['ALTER_WORD'].isnull()].index ,inplace=True)
    alter_df.drop(alter_df[alter_df['WORD'].isnull()].index ,inplace=True)
    
    check = pd.read_excel('CLASS_CLEAN_FINAL.xlsx')
    
    check_df = pd.ExcelFile('CLASS_CLEAN_FINAL.xlsx')       # class data with special cases
    check1 = pd.read_excel(check_df ,'Sheet2')
    
    # ALTER WORD REPLACEMENT
    def altr_word(des):
        des = re.sub(r'[^A-Za-z0-9\s\.\/,;&:\-]' ,' ' ,des)                       # Replace characters other than listed with space
        des = re.sub(r' +' ,' ' ,des)                                             # Trim extra spaces
        x = des.upper()
        for word ,altr in zip(alter_df['WORD'] ,alter_df['ALTER_WORD']):
            if (altr in x) & (len(altr ) >2) :                                    # check for alter word in des and alter word len > 2 and replace
                altr = r'\b{}\b'.format(altr)
                x = re.sub(altr ,word ,x)
        return x
    
    # BASED ON RULE1
    def rule1(x):
        r1_cls = []
        print('---> ', x)
        for cls, r1 in zip(check['Class'], check['RULE1']):
            # print('CLS---> ', cls)
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            mat = re.search(r'(\bFOR\b|F/|W/|\bWITH\b)', x)
            if mat:
                b = re.sub(pat ,'' ,x)
                if ',' or ';' in b:
                    b = re.sub(r'(,|;).*' ,'' ,b)                                     # considering description before ',' or ';'
                    if re.search(r'\b({})\b'.format(r1) ,b):
                        r1_cls.append(cls)
                    # else:
                    #     pass
                elif re.search(r'\b({})\b'.format(r1) ,b):
                    r1_cls.append(cls)
                else:
                    pass

                    # if re.search(r'\b({})\b'.format(r1) ,b):
                    #     r1_cls.append(cls)
            elif ',' or ';' in x:
                x = re.sub(r'(,|;).*' ,'' ,x)                                         # considering description before ',' or ';'
                if re.search(r'\b({})\b'.format(r1) ,x):
                    r1_cls.append(cls)

            else:
                if re.search(r'\b({})\b'.format(r1) ,x):
                    r1_cls.append(cls)
        lst = [re.search(r'[A-Z].*(,|\s)[A-Z].*' ,i).group() for i in r1_cls if re.search(r',|\s' ,i)]            # listing classes containing ',' and space
        splt_lst = [i.split() for i in lst]
        splt_lst = [j for i in splt_lst for j in i]
        splt_lst = [re.sub(r',' ,'' ,i) for i in splt_lst]
        fnl_cls = [x for x in r1_cls if x not in splt_lst]                      # checking for generics in lst and remove them
        rule_response = None
        if len(fnl_cls ) == 1:
            # print('LINENO: 58  ', fnl_cls[0])
            rule_response = fnl_cls[0]
        else:
            rule_response = ''
        # print('<---', rule_response, '--->')
        # rule_response = 'DUMMY'
        # del lst, splt_lst, fnl_cls, r1_cls, mat, b
        return rule_response
    
    def obj_qual_cndtn(x):    
            # search object without synonym in des before ',' ';'
        objct = []
        for cls, obj ,qual in zip(check['Class'] ,check['TERM(OBJECT)'] ,check['TERM(Qualifier)']):
            if re.search(r'\b({})\b'.format(obj) ,x):                                                        # Searching for object in des
                if '|' in obj:
                    objct.append \
                        (re.search(r'\b({})\b'.format(obj) ,x).group())                              # append match group to list if '|' in obj
                else:
                    objct.append(obj)    
            
        obj_list = []
        for i in objct:
            if i not in obj_list:
                obj_list.append(i)                                                                          # Creating auxilary list such that repeated objs are removed 
        x = re.sub(r'[^A-Za-z0-9\s]' ,' ' ,x)                                                                 # Considering des without special characters
        x = re.sub(r' +' ,' ' ,x)                                                                             # Trimming extra spaces
        if len(obj_list ) == 1:
            return obj_list[0]                                                                              # If len of list is 1 then return list obj
        elif len(obj_list ) >1:
            splt = x.split(' ')                                                                             # Else split the des
            indx = [splt.index(idx) for idx in obj_list if idx in splt]                                     # Indexing each split des with obj positions
            if indx != []:
                indx.sort()                                                                                 # sorting the index
                if indx == list(range(min(indx), max(indx ) +1)):
                    return splt[max(indx)]                                                                  # If objs in sequence then return object from right end
                else:
                    return splt[min(indx)]                                                                  # Else return obj from left end
        else:
            return None
    
    def object(des):
        # split description with (FOR, WITH, F/, W/)
        pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
        mat = re.search(r'(\bFOR\b|F/|W/|\bWITH\b)' ,des)
        if mat:                                                                                             
            # des before split terms
            b = re.sub(pat ,'' ,des)
            if (',' or ';') in b:                                                                           
                # des before ',' ';'
                x = re.sub(r'(,|;).*' ,'' ,b)
                return obj_qual_cndtn(x)
                # if ',' ';'doesnt exist in des
            else:
                return obj_qual_cndtn(b)
        # same procedure with des without (FOR,WITH,F/,W/)
        else:
            if (',' or ';') in des:
                # des before ',' ';'
                x = re.sub(r'(,|;).*' ,'' ,des)
                return obj_qual_cndtn(x)
            # if ',' ';'doesnt exist in des
            else:
                return obj_qual_cndtn(des)
    
    def obj_actual(ob):
        for cls ,term in zip(check['Class'] ,check['TERM(OBJECT)']):
            trm = re.split(r'\|' ,term)                                                  # Searching for as is object
            if (ob in trm) and (ob == cls):
                return term
    def obj_synonym(ob):
        for term in check['TERM(OBJECT)']:
            trm = re.split(r'\|' ,term)                                                  # Searching for objects along with synonyms
            if ob in trm:
                return term
    def fnl_obj(ob1 ,ob2):                                                               # Allocating final object from obj1, obj2 based on conditions
        if ob1 == ob2:
            return ob1
        elif ob1 is None:
            return ob2
        elif ob2 is None:
            return ob1
        elif re.search(ob1 ,ob2):
            return re.search(ob1 ,ob2).group()
    
    def r1_qualifier1(x ,obj):
        if ' ' in obj:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if (re.search(r'\b{}\b'.format(obj) ,i)) and
                                                        (re.search(r'(\s)', i))]) & check['TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(r'(\bFOR\b|F/|W/|\bWITH\b)', x)
                if mat:
                    b = re.sub(pat, '', x)
                    # if re.search(r'\b({})\b'.format(obj),b):
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                            cls):  # if obj before '|', qual in class and qual in des then return qual
                        if re.search(r'\b({})\b'.format(qual), b):
                            return qual
                        # else:
                        #     return None
                else:
                    # if re.search(r'\b({})\b'.format(obj),x):
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), x):
                            return qual
                        # else:
                        #     return None
        else:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        not re.search(r'(\s)', i))]) & check[
                            'TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(r'(\bFOR\b|F/|W/|\bWITH\b)', x)
                if mat:
                    b = re.sub(pat, '', x)
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), b):
                            return qual
                else:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), x):
                            return qual

    def r1_qualifier2(x, obj):
        # chk = check[check['TERM(OBJECT)']==obj]
        if ' ' in obj:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        re.search(r'(\s)', i))]) & check['TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pat, x)
                if mat:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), mat.group()):
                            return qual
        else:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        not re.search(r'(\s)', i))]) & check[
                            'TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pat, x)
                if mat:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), mat.group()):
                            return qual

    def obj_qualifier1(x, obj):
        if ' ' in obj:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        re.search(r'(\s)', i))]) & check['TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(r'(\bFOR\b|F/|W/|\bWITH\b)', x)
                if mat:
                    b = re.sub(pat, '', x)
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), b):
                            return qual
                else:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), x):
                            return qual
        else:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        not re.search(r'(\s)', i))]) & check[
                            'TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(r'(\bFOR\b|F/|W/|\bWITH\b)', x)
                if mat:
                    b = re.sub(pat, '', x)
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), b):
                            return qual
                else:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), x):
                            return qual

    def obj_qualifier2(x, obj):
        if ' ' in obj:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        re.search(r'(\s)', i))]) & check['TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pat, x)
                if mat:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), mat.group()):
                            return qual
        else:
            chk = check[check['TERM(OBJECT)'].isin([i for i in check['TERM(OBJECT)'] if
                                                    (re.search(r'\b{}\b'.format(obj), i)) and (
                                                        not re.search(r'(\s)', i))]) & check[
                            'TERM(Qualifier)'].notnull()]
            pat = r'(\bFOR\b|F/|W/|\bWITH\b).*'
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pat, x)
                if mat:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), mat.group()):
                            return qual

    def fnl_qual(qu1, qu2):  # Allocating final Qualifier from qual1, qual2 based on conditions
        if qu1 == qu2:
            return qu1
        elif qu1:
            return qu1
        elif qu2:
            return qu2
        else:
            return None

    def fnl_cls(ob, qu1, qu2):
        for cls, obj, qua in zip(check['Class'], check['TERM(OBJECT)'], check['TERM(Qualifier)']):
            if qu1:
                if ',' in cls:
                    ##### Searching for obj and qual in cls; joining them as obj, qual then comparing with class, if matches then assign corresponding class #####
                    if (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(,).*', '', cls))) and (
                    re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)', '', cls)), qu1)):
                        if ', '.join([re.search(ob, re.sub(r'(,).*', '', cls)).group(),
                                      re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)', '', cls)), qu1).group()]) == cls:
                            return cls
                            ##### If combination of obj, qual doesnt exist then return only obj #####
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                            else:
                                return ob
                else:
                    ##### Searching for obj and qual in cls; joining them as obj qual then comparing with class, if matches then assign corresponding class #####
                    if (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(\s).*', '', cls))) and (
                    re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu1)):
                        if ' '.join([re.search(ob, re.sub(r'(\s).*', '', cls)).group(),
                                     re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu1).group()]) == cls:
                            return cls
                        ##### If combination of obj qual doesnt exist then return only obj #####
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                            else:
                                return ob
            elif qu2:
                if ',' in cls:
                    if (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(,).*', '', cls))) and (
                    re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)', '', cls)), qu2)):
                        if ', '.join([re.search(ob, re.sub(r'(,).*', '', cls)).group(),
                                      re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)', '', cls)), qu2).group()]) == cls:
                            return cls
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                            else:
                                return ob
                else:
                    if (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(\s).*', '', cls))) and (
                    re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu2)):
                        if ' '.join([re.search(ob, re.sub(r'(\s).*', '', cls)).group(),
                                     re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu2).group()]) == cls:
                            return cls
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                            else:
                                return ob
            ####### If qual1, qual2 are None and obj is same as class then return obj
            else:
                if ob == cls:
                    return ob
                ######## If '|' in obj then check 1st word before '|' is same as class or not and return it
                elif '|' in ob:
                    if (ob == obj) and (re.sub(r'(\|).*', '', r'{}\b'.format(ob)) == cls):
                        return re.sub(r'(\|).*', '', r'{}\b'.format(ob))

    ####### Function to extract class by checking the description terms in source des and if all terms exist(in any order) then only class will be assigned ######## 
    def frst_cls(des, cls, st_des):
        lst = []
        st = st_des.split()
        for i in st:
            if re.search(r'\b{}\b'.format(i), des):  # Split the table des and check each term in source des
                lst.append(i)
            else:
                lst.append('')
        if '' not in lst:
            return cls
        else:
            return None

    def frst_chk(des):
        for chk_des, cls in zip(check1['DESCRIPTION'], check1['CLASS']):
            if frst_cls(des, cls, chk_des):
                return frst_cls(des, cls, chk_des)

    ###### Replacing alter words using altr_word function and creating New+_description #######
    #--------------Step 1---------------------------
    # source_DF['New_Source_Long_Description'] = source_DF['SHORT_DESCRIPTION'].apply(lambda des: altr_word(des))
    source_DF['New_Source_Long_Description'] = source_DF['LONG_DESCRIPTION'].apply(lambda des: altr_word(des))

    #-------------------------------------------------

    #--------------Step 2---------------------------
    source_DF['CLASS_TYPE1'] = source_DF['New_Source_Long_Description'].apply(lambda des: rule1(des))
    source_DF['CLASS_TYPE1'] = ''
    # for ky, val in source_DF['New_Source_Long_Description'].items():
    #     print('------->', ky, '|', val)
    #     source_DF['CLASS_TYPE1'][ky] = rule1(val)

    print('CLASS_TYPE1--->>>', source_DF['CLASS_TYPE1'])
    #-------------------------------------------------


    # #--------------Step 3---------------------------
    source_DF['Object_'] = source_DF[source_DF['CLASS_TYPE1'].isnull()]['New_Source_Long_Description'].apply(
        lambda des: object(des))
    #-------------------------------------------------

    #--------------Step 4---------------------------
    source_DF['Object1'] = source_DF[source_DF['Object_'].notnull()]['Object_'].apply(lambda x: obj_synonym(x))
    #-------------------------------------------------

    #--------------Step 5---------------------------
    source_DF['Object2'] = source_DF[source_DF['Object_'].notnull()]['Object_'].apply(lambda x: obj_actual(x))
    #-------------------------------------------------


    #--------------Step 6---------------------------
    try:
        source_DF['Object'] = source_DF[source_DF['Object_'].notnull()].apply(lambda x: fnl_obj(x.Object1, x.Object2),
                                                                              axis=1)
    except:
        source_DF['Object'] = np.NaN
    #-------------------------------------------------

    #--------------Step 7---------------------------
    source_DF['R1_Qualifier1'] = source_DF[source_DF['CLASS_TYPE1'].notnull()].apply(
        lambda x: r1_qualifier1(x.New_Source_Long_Description, x.CLASS_TYPE1), axis=1)
    #-------------------------------------------------

    #--------------Step 8---------------------------
    source_DF['R1_Qualifier2'] = source_DF[
        source_DF['CLASS_TYPE1'].notnull() & source_DF['R1_Qualifier1'].isnull()].apply(
        lambda x: r1_qualifier2(x.New_Source_Long_Description, x.CLASS_TYPE1), axis=1)
    #-------------------------------------------------

    #--------------Step 9---------------------------
    try:
        source_DF['Obj_Qualifier1'] = source_DF[source_DF['Object'].notnull()].apply(
            lambda x: obj_qualifier1(x.New_Source_Long_Description, x.Object), axis=1)
        source_DF['Obj_Qualifier2'] = source_DF[
            source_DF['Object'].notnull() & source_DF['Obj_Qualifier1'].isnull()].apply(
            lambda x: obj_qualifier2(x.New_Source_Long_Description, x.Object), axis=1)
    except:
        source_DF['Obj_Qualifier1'] = np.NaN
        source_DF['Obj_Qualifier2'] = np.NaN
    # -------------------------------------------------

    #--------------Step 10---------------------------
    source_DF['CLASS1'] = source_DF[source_DF['CLASS_TYPE1'].notnull()].apply(
        lambda x: fnl_cls(x.CLASS_TYPE1, x.R1_Qualifier1, x.R1_Qualifier2), axis=1)
    #-------------------------------------------------

    #--------------Step 11---------------------------
    try:
        source_DF['CLASS2'] = source_DF[source_DF['Object'].notnull()].apply(
            lambda x: fnl_cls(x.Object, x.Obj_Qualifier1, x.Obj_Qualifier2), axis=1)
    except:
        source_DF['CLASS2'] = np.NaN
    #-------------------------------------------------



    #-----------------------------------Level 2-------------------------------
    ###### Extyracting classes with special cases #######
    source_DF['CLASS_TYPE0'] = source_DF[source_DF['CLASS1'].isnull() & source_DF['CLASS2'].isnull()][
        'New_Source_Long_Description'].apply(lambda des: frst_chk(des))

    ###### Combining two class columns to one class column ######
    source_DF['CLASS'] = source_DF[['CLASS1', 'CLASS2', 'CLASS_TYPE0']].apply(
        lambda x: '{}{}{}'.format(x[0], x[1], x[2]), axis=1)
    source_DF['CLASS'] = [re.sub(r'(None|nan)', '', x) for x in source_DF['CLASS']]

    ###### Cross checking obtained class with actual class ######
    cross_chk_Df = source_DF[source_DF['CLASS'].notnull()]
    cross_chk_Df['CLASS'] = cross_chk_Df['CLASS'].apply(
        lambda x: 'No Class' if x in cross_chk_Df[cross_chk_Df['CLASS'].isin(check['Class']) == False][
            'CLASS'].tolist() else x)
    source_DF.update(cross_chk_Df)
    source_DF.replace('No Class', np.NaN, inplace=True)
    #--------------------------------------------------------------------

    ####### Retaining Description and Class column and dropping off other columns
    # source_DF.drop(['New_Source_Long_Description','Object_','CLASS_TYPE1','CLASS_TYPE0','Object1','Object2','Object','Obj_Qualifier1','Obj_Qualifier2','R1_Qualifier1','R1_Qualifier2','CLASS1','CLASS2'],axis=1,inplace=True)
    source_DF.fillna('', inplace=True)
    # source_DF.to_excel('UNS_CLS.xlsx')
    print('Response yet to send...', '\n', source_DF)
    source_DF = source_DF[['MATERIAL_NO', 'CLASS_TYPE1', 'CLASS']]
    return source_DF
    # sys.exit(None)
    # return source_DF



