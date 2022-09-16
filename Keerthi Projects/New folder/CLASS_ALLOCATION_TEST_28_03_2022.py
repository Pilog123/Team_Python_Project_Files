

import pandas as pd
import numpy as np

import re

import cx_Oracle




# MDRM_V10_TEST:MDRM#vten_tesT@172.16.1.13:1521/?service_name=DP071201

def class_allocation_func(source_DF):
    source_DF = source_DF[source_DF['LONG_TEXT'].notnull()]

    # CLASS TABLE
    check_df = pd.ExcelFile(r'CLASS_CLEAN_Item_Details.xlsx')

    class_check = pd.read_excel(check_df, 'Sheet1')

    check1 = pd.read_excel(check_df, 'Sheet2')

    # DF = pd.read_excel(r'./Item Details/O_RECORD_UNSPSC_CLOUD_22_03_2022.xls')

    alter_df = pd.read_excel(r'IS_ALTER_WORD_ABB_FINAL.xlsx')
    alter_df.drop(alter_df[alter_df['ALTER_WORD'].isnull()].index, inplace=True)
    alter_df.drop(alter_df[alter_df['WORD'].isnull()].index, inplace=True)

    ### Exclusions ###
    exmp = pd.ExcelFile(r'Exclusions.xlsx')
    exmp_df = pd.read_excel(exmp, 'Sheet2')
    exmp_cls_df = pd.read_excel(exmp, 'Sheet1')
    exmp_lst_df = pd.read_excel(exmp, 'Sheet3')

    r1_check = pd.read_excel(check_df, 'Sheet3')
    obj_check = class_check[class_check['TERM(Qualifier)'].notnull()]
    lst_check = pd.read_excel(check_df, 'Sheet4')
    pattern = r'(\bFOR\b|F/|W/|\bWITH\b|\bAT\b|\bINCLUDE\b|\bINCLUDING\b|\bAND\b|\bCONSISTS\b|\bINCLUDES\b)'

    # ALTER WORD REPLACEMENT
    def altr_word(des):
        des = re.sub(r'[^A-Za-z0-9\s\.\/,;&:\(\)\']', ' ', des)
        des = re.sub(r' +', ' ', des)
        des = re.sub(r'\n', ' ', des)
        x = des.upper()
        for word, altr in zip(alter_df['WORD'], alter_df['ALTER_WORD']):
            if (altr in x) & (len(altr) > 2):
                altr = r'\b{}\b'.format(altr)
                x = re.sub(altr, word, x)
        return x

    def exclusion_cndtn(des, lst, splt):
        sp = []
        ls = []
        for exmp_obj, trm in zip(exmp_df['Object'], exmp_df['Exemptions']):
            for exmp_trm in trm.split('|'):
                if ((re.search(r'\b{}\b'.format(exmp_obj), des)) and (re.search(r'\b{}\b'.format(exmp_trm), des))) and (
                        re.search(r'\b{}\b'.format(exmp_trm), des).group() in splt):
                    if des.index(exmp_obj) < des.index(re.search(r'\b{}\b'.format(exmp_trm), des).group()):
                        sp.append(re.search(r'\b{}\b'.format(exmp_trm), des).group())
                        ls.append(re.search(r'\b{}\b'.format(exmp_trm), des).group())
        l = [i for i in lst if i not in sp]
        s = [i for i in splt if i not in ls]
        return l, s

    def rule1_cndtn(des):
        r1_cls = []
        ltr_lst = []

        ltr_lst = set([s[0] for s in des.split()])
        r1_fltrd = pd.DataFrame()
        for i in ltr_lst:
            r1_samp = r1_check.loc[r1_check['RULE1'].str.startswith(i, na=False)]
            r1_fltrd = pd.concat([r1_fltrd, r1_samp])
            for cls, r1 in zip(r1_fltrd['Class'], r1_fltrd['RULE1']):
                if re.search(r'\b({})\b'.format(r1), des):
                    r1_cls.append([cls, re.search(r'\b({})\b'.format(r1), des).group()])
        r1_list = []
        for i in r1_cls:
            if i not in r1_list:
                r1_list.append(i)
        r1_str_list = [i[1] for i in r1_list]
        r1_cls_list = [i[0] for i in r1_list]

        if len(r1_str_list) == 1:

            for exc in exmp_lst_df['Exclusions']:
                if re.search(r'\b{}\b'.format(r1_cls_list[0].split(', ')[0]), exc):
                    return None
            else:

                return [r1_cls_list[0], r1_str_list[0]]

        elif len(r1_str_list) > 1:
            r1_ind = [des.index(i) for i in r1_str_list]
            fltr_obj = [re.sub(r'(,|\s).*', '', o) for o in r1_cls_list]
            for exmp_cls, trm in zip(exmp_cls_df['Class'], exmp_cls_df['Exemptions']):
                ex = []
                for exmp_trm in trm.split('|'):
                    if ((exmp_cls in r1_cls_list) and (re.search(r'\b{}\b'.format(exmp_trm), des))) and (
                            re.search(r'\b{}\b'.format(exmp_trm), des).group() in fltr_obj):
                        fltr_obj.remove(re.search(r'\b{}\b'.format(exmp_trm), des).group())

                        ex.append(re.search(r'\b{}\b'.format(exmp_trm), des).group())
            flt_ob = [i for i in fltr_obj if i not in ex]

            fltr_lst = []
            for i in fltr_obj:
                fltr_lst.extend([word for word in r1_cls_list if word.startswith(i)])
            if len(fltr_lst) == 1:
                for exc in exmp_lst_df['Exclusions']:
                    if re.search(r'\b{}\b'.format(fltr_lst[0].split(', ')[0]), exc):
                        return None
                else:
                    return [fltr_lst[0], r1_str_list[0]]
            else:
                for exc in exmp_lst_df['Exclusions']:
                    if re.search(r'\b{}\b'.format(fltr_lst[0].split(', ')[0]), exc):
                        return None
                else:
                    return [fltr_lst[r1_ind.index(max(r1_ind))], r1_str_list[r1_ind.index(max(r1_ind))]]
        else:
            return None

    def rule1(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +', ' ', des)

        pat = r'{}.*'.format(pattern)
        mat = re.search(pattern, des)
        if mat:
            x = re.sub(pat, '', des)
            if ',' or ';' in x:
                b = re.sub(r'(,|;).*', '', x)
                return rule1_cndtn(b)
            else:
                return rule1_cndtn(x)

        elif ',' or ';' in des:
            x = re.sub(r'(,|;).*', '', des)
            return rule1_cndtn(x)

        else:
            return rule1_cndtn(des)

    def obj_qual_cndtn(x):
        objct = []
        for cls, obj, qual in zip(class_check['Class'], class_check['TERM(OBJECT)'], class_check['TERM(Qualifier)']):
            if re.search(r'\b({})\b'.format(obj), x):
                if '|' in obj:
                    objct.append(re.search(r'\b({})\b'.format(obj), x).group())
                else:
                    objct.append(obj)
        obj_list = []
        for i in objct:
            if i not in obj_list:
                obj_list.append(i)
        x = re.sub(r'[^A-Za-z0-9\s\.\/,;&:\(\)\'\-]', ' ', x)
        x = re.sub(r' +', ' ', x)
        if len(obj_list) == 1:
            return obj_list[0]
        elif len(obj_list) > 1:
            splt = x.split(' ')
            ob_lst, splt = exclusion_cndtn(x, obj_list, splt)

            def string_set(string_list):
                return set(i for i in string_list
                           if not any(i in s for s in string_list if (i != s and ' ' in s)))

            ob_str = list(string_set(ob_lst))
            obj_str = [re.sub(r'\^', '', i) for i in ob_str]

            if obj_str != []:
                indx = [x.index(i) for i in obj_str]
                return obj_str[indx.index(max(indx))]
        else:
            return None

    def object(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +', ' ', des)

        # split description with (FOR, WITH, F/, W/)
        pat = r'{}.*'.format(pattern)
        mat = re.search(pattern, des)
        if mat:
            # des before split terms
            b = re.sub(pat, '', des)
            if ',' or ';' in b:
                # des before ',' ';'
                x = re.sub(r'(,|;).*', '', b)
                return obj_qual_cndtn(x)
                # if ',' ';'doesnt exist in des
            else:
                return obj_qual_cndtn(b)
        # same procedure with des without (FOR,WITH,F/,W/)
        else:
            if ',' or ';' in des:
                # des before ',' ';'
                x = re.sub(r'(,|;).*', '', des)
                return obj_qual_cndtn(x)
            # if ',' ';'doesnt exist in des
            else:
                return obj_qual_cndtn(des)

    def obj_actual(ob):
        for cls, term in zip(class_check['Class'], class_check['TERM(OBJECT)']):
            trm = re.split(r'\|', term)
            if '^' in ob:
                return re.sub(r'\^', '', ob)
            elif (ob in trm) and (ob == cls):
                return term

    def obj_synonym(ob):
        for term in class_check['TERM(OBJECT)']:
            trm = re.split(r'\|', term)
            if ('^' in term) and (r'^{}'.format(ob) in term):
                return term
            elif ob in trm:
                return term

    def fnl_obj(ob1, ob2):
        if ob1 == ob2:
            return ob1
        elif ob1 is None:
            return ob2
        elif ob2 is None:
            return ob1
        elif re.search(ob1, ob2):
            return re.search(ob1, ob2).group()
        elif ob1 is None and ob2 is None:
            return None

    def obj_qualifier1(x, obj):
        qual1 = []
        qual1_str = []
        if ' ' in obj:
            chk = obj_check[obj_check['TERM(OBJECT)'].isin([i for i in obj_check['TERM(OBJECT)'] if
                                                            (re.search(r'\b{}\b'.format(obj), i)) and (
                                                                re.search(r'(\s)', i))]) & obj_check[
                                'TERM(Qualifier)'].notnull()]
            pat = r'{}.*'.format(pattern)

            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pattern, x)
                if mat:
                    b = re.sub(pat, '', x)
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), b):
                            qual1.append(qual)
                            qual1_str.append(re.search(r'\b({})\b'.format(qual), b).group())
                else:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), x):
                            qual1.append(qual)
                            qual1_str.append(re.search(r'\b({})\b'.format(qual), x).group())

            if qual1_str != []:
                qual1_ind = [x.index(i) for i in qual1_str]
                return qual1[qual1_ind.index(min(qual1_ind))]
        else:
            chk = obj_check[obj_check['TERM(OBJECT)'].isin([i for i in obj_check['TERM(OBJECT)'] if
                                                            (re.search(r'\b{}\b'.format(obj), i)) and (
                                                                not re.search(r'(\s)', i))]) & obj_check[
                                'TERM(Qualifier)'].notnull()]
            pat = r'{}.*'.format(pattern)

            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pattern, x)
                if mat:
                    b = re.sub(pat, '', x)
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), b):
                            qual1.append(qual)
                            qual1_str.append(re.search(r'\b({})\b'.format(qual), b).group())
                else:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), x):
                            qual1.append(qual)
                            qual1_str.append(re.search(r'\b({})\b'.format(qual), x).group())

            if qual1_str != []:
                qual1_ind = [x.index(i) for i in qual1_str]
                return qual1[qual1_ind.index(min(qual1_ind))]

    def obj_qualifier2(x, obj):
        qual2 = []
        qual2_str = []
        if ' ' in obj:
            chk = obj_check[obj_check['TERM(OBJECT)'].isin([i for i in obj_check['TERM(OBJECT)'] if
                                                            (re.search(r'\b{}\b'.format(obj), i)) and (
                                                                re.search(r'(\s)', i))]) & obj_check[
                                'TERM(Qualifier)'].notnull()]
            pat = r'{}.*'.format(pattern)
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pat, x)
                if mat:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), mat.group()):
                            qual2.append(qual)
                            qual2_str.append(re.search(r'\b({})\b'.format(qual), mat.group()).group())
        else:
            chk = obj_check[obj_check['TERM(OBJECT)'].isin([i for i in obj_check['TERM(OBJECT)'] if
                                                            (re.search(r'\b{}\b'.format(obj), i)) and (
                                                                not re.search(r'(\s)', i))]) & obj_check[
                                'TERM(Qualifier)'].notnull()]
            pat = r'{}.*'.format(pattern)
            for cls, ob, qual in zip(chk['Class'], chk['TERM(OBJECT)'], chk['TERM(Qualifier)']):
                mat = re.search(pat, x)
                if mat:
                    if re.search(r'\b({}, {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual),
                                 cls) or re.search(
                            r'\b({} {})\b'.format(re.sub(r'(\|).*', '', r'{}\b'.format(obj)), qual), cls):
                        if re.search(r'\b({})\b'.format(qual), mat.group()):
                            qual2.append(qual)
                            qual2_str.append(re.search(r'\b({})\b'.format(qual), mat.group()).group())
        if qual2_str != []:
            qual2_ind = [x.index(i) for i in qual2_str]
            return qual2[qual2_ind.index(min(qual2_ind))]

    def fnl_qual(qu1, qu2):
        if qu1 == qu2:
            return qu1
        elif qu1:
            return qu1
        elif qu2:
            return qu2
        else:
            return None

    def fnl_cls(ob, qu1, qu2):
        for cls, obj, qua in zip(class_check['Class'], class_check['TERM(OBJECT)'], class_check['TERM(Qualifier)']):
            if qu1:
                if ',' in cls:
                    if (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(,).*', '', cls))) and (
                    re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)', '', cls)), qu1)):
                        if ', '.join([re.search(ob, re.sub(r'(,).*', '', cls)).group(),
                                      re.search(r'\b{}\b'.format(re.sub(r'.*(,)(\s)', '', cls)), qu1).group()]) == cls:
                            return cls
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                            else:
                                return ob
                else:
                    if (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(\s).*', '', cls))) and (
                    re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu1)):
                        if ' '.join([re.search(ob, re.sub(r'(\s).*', '', cls)).group(),
                                     re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu1).group()]) == cls:
                            return cls
                        else:
                            if '|' in ob:
                                return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                            else:
                                return ob
            elif qu2:
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
                elif (re.search(r'\b{}\b'.format(str(ob)), re.sub(r'(\s).*', '', cls))) and (
                re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu2)):
                    if ' '.join([re.search(ob, re.sub(r'(\s).*', '', cls)).group(),
                                 re.search(r'\b{}\b'.format(re.sub(r'.*(\s)', '', cls)), qu2).group()]) == cls:
                        return cls
                    else:
                        if '|' in ob:
                            return re.sub(r'(\|).*', '', r'{}\b'.format(ob))
                        else:
                            return ob
            else:
                if (ob == cls):
                    return ob
                elif '|' in ob:
                    if (ob == obj) and (re.sub(r'(\|).*', '', r'{}\b'.format(ob)) == cls):
                        return re.sub(r'(\|).*', '', r'{}\b'.format(ob))

    def frst_cls(des, cls, st_des):
        pat = r'(\bFOR\b|F/|W/|\bWITH\b|\bAT\b|\bINCLUDE\b|\bINCLUDING\b|\bAND\b|\bCONSISTS\b|\bINCLUDES\b|,|;)'
        lst = []
        st = st_des.split()
        if re.search(r'\b{}\b'.format(st[0]), re.sub(r'{}.*'.format(pat), '', des)):
            lst.append(st[0])
            for i in st[1:]:
                if re.search(r'\b{}\b'.format(i), des):
                    lst.append(i)
                else:
                    lst.append('')
        if ('' not in lst) & (lst != []):
            return cls
        else:
            return None

    def frst_chk(des):
        for chk_des, cls in zip(check1['DESCRIPTION'], check1['CLASS']):
            if frst_cls(des, cls, chk_des):
                return frst_cls(des, cls, chk_des)

    def last_cls(des):
        des = re.sub(r'\([^()]*\)', ' ', des)
        des = re.sub(r' +', ' ', des)
        for lst_cls, lst_obj in zip(lst_check['Class'], lst_check['TERM(OBJECT)']):
            pat = r'{}.*'.format(pattern)
            mat = re.search(pattern, des)
            if mat:
                b = re.sub(pat, '', des)
                if ',' or ';' in b:
                    b = re.sub(r'(,|;).*', '', b)
                    if re.search(r'\b({})\b'.format(lst_obj), b):
                        return lst_cls
                else:
                    if re.search(r'\b({})\b'.format(lst_obj), b):
                        return lst_cls
            elif ',' or ';' in des:
                x = re.sub(r'(,|;).*', '', des)
                if re.search(r'\b({})\b'.format(lst_obj), x):
                    return lst_cls
            else:
                if re.search(r'\b({})\b'.format(lst_obj), des):
                    return lst_cls

    def final_class(des, r1, r1_cls, ob, obj_cls):
        if (r1_cls is not np.NaN) and (obj_cls is not np.NaN):
            if des.index(r1) < des.index(ob):
                return obj_cls
            else:
                return r1_cls
        elif r1_cls is not np.NaN and obj_cls is np.NaN:
            return r1_cls
        elif r1_cls is np.NaN and obj_cls is not np.NaN:
            return obj_cls
        else:
            return None

            ###### Replacing alter words using altr_word function and creating New+_description #######

    # source_DF['New_Source_Long_Description'] = source_DF['SHORT_DESCRIPTION'].apply(lambda des:altr_word(des))

    source_DF['New_Source_Long_Description'] = source_DF['LONG_TEXT'].apply(lambda des: altr_word(des))

    source_DF['RULE1'] = source_DF['New_Source_Long_Description'].apply(lambda des: rule1(des))

    df1 = source_DF[source_DF['RULE1'].isnull()]
    df2 = source_DF[source_DF['RULE1'].notnull()]
    try:
        df2[['CLASS_TYPE1', 'RULE1_STRING']] = pd.DataFrame(df2.RULE1.tolist(), index=df2.index)
    except:
        df2[['CLASS_TYPE1', 'RULE1_STRING']] = None, None
    source_DF = pd.concat([df1, df2], ignore_index=True)

    source_DF['Object_'] = source_DF['New_Source_Long_Description'].apply(lambda des: object(des))

    source_DF['Object1'] = source_DF[source_DF['Object_'].notnull()]['Object_'].apply(lambda x: obj_synonym(x))
    source_DF['Object2'] = source_DF[source_DF['Object_'].notnull()]['Object_'].apply(lambda x: obj_actual(x))

    try:
        source_DF['Object'] = source_DF[source_DF['Object_'].notnull()].apply(lambda x: fnl_obj(x.Object1, x.Object2),
                                                                              axis=1)
    except:
        source_DF['Object'] = None

    try:
        source_DF['Obj_Qualifier1'] = source_DF[source_DF['Object'].notnull()].apply(
            lambda x: obj_qualifier1(x.New_Source_Long_Description,
                                     x.Object), axis=1)
        source_DF['Obj_Qualifier2'] = source_DF[source_DF['Object'].notnull() &
                                                source_DF['Obj_Qualifier1'].isnull()].apply(
            lambda x: obj_qualifier2(x.New_Source_Long_Description,
                                     x.Object), axis=1)
    except:
        source_DF['Obj_Qualifier1'] = None
        source_DF['Obj_Qualifier2'] = None

    try:
        source_DF['CLASS2'] = source_DF[source_DF['Object'].notnull()].apply(
            lambda x: fnl_cls(x.Object, x.Obj_Qualifier1, x.Obj_Qualifier2), axis=1)
    except:
        source_DF['CLASS2'] = None

    source_DF['FINAL_CLASS'] = source_DF.apply(
        lambda x: final_class(x.New_Source_Long_Description, x.RULE1_STRING, x.CLASS_TYPE1,
                              x.Object_, x.CLASS2), axis=1)

    ###### Extracting classes with special cases #######
    source_DF['CLASS_TYPE0'] = source_DF[source_DF['FINAL_CLASS'].isnull()]['New_Source_Long_Description'].apply(
        lambda des: frst_chk(des))

    source_DF['CLASS_LAST'] = source_DF[source_DF['FINAL_CLASS'].isnull() &
                                        source_DF['CLASS_TYPE0'].isnull()]['New_Source_Long_Description'].apply(
        lambda des: last_cls(des))

    ###### Combining two class columns to one class column ######
    source_DF['NEW_CLASS'] = source_DF[['CLASS_TYPE0', 'FINAL_CLASS', 'CLASS_LAST']].apply(
        lambda x: '{}{}{}'.format(x[0], x[1], x[2]),
        axis=1)
    source_DF['NEW_CLASS'] = [re.sub(r'(None|nan)', '', x) for x in source_DF['NEW_CLASS']]

    ###### Cross checking obtained class with actual class ######
    cross_chk_Df = source_DF[source_DF['NEW_CLASS'].notnull()]
    total_class_df = pd.concat([class_check, lst_check], ignore_index=True)
    cross_chk_Df['NEW_CLASS'] = cross_chk_Df['NEW_CLASS'].apply(
        lambda x: 'No Class' if x in cross_chk_Df[cross_chk_Df['NEW_CLASS'].isin(total_class_df['Class']) == False][
            'NEW_CLASS'].tolist() else x)
    source_DF.update(cross_chk_Df)
    source_DF.replace('No Class', np.NaN, inplace=True)

    ####### Retaining Description and Class column and dropping off other columns
    source_DF.drop(
        ['New_Source_Long_Description', 'Object_', 'Object1', 'Object2', 'Object', 'Obj_Qualifier1', 'Obj_Qualifier2',
         'RULE1', 'CLASS_TYPE1',
         'RULE1_STRING', 'CLASS2', 'CLASS_TYPE0', 'CLASS_LAST', 'FINAL_CLASS'], axis=1, inplace=True)

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
    unspsc = pd.read_sql('''SELECT TERM,UNSPSC_CODE,UNSPSC_DESC,UNSPSC_CODE_SEGMENT_LEVEL,UNSPSC_DESC_SEGMENT_LEVEL,UNSPSC_CODE_FAMILY_LEVEL,
                            UNSPSC_DESC_FAMILY_LEVEL,UNSPSC_CODE_CLASS_LEVEL,UNSPSC_DESC_CLASS_LEVEL,ISIC_CODE,
                            ISIC_CODE_DESC, HSN_HTS_CODE, HSN_HTS_DESC FROM ONTG_CLASSIFICATION''', conn)
    print('UNSPSC_DATA: ', unspsc)
    conn.close()

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

    source_dataframe['ISIC_CLASS_DATA'] = source_dataframe['NEW_CLASS'].apply(
        lambda cls: unspsc_ISIC_class(cls))  # @# NEw
    source_dataframe['HSN_CLASS_DATA'] = source_dataframe['NEW_CLASS'].apply(
        lambda cls: unspsc_HSN_class(cls))  # @# NEw

    print(source_dataframe)
    return source_dataframe.fillna('')


# source_DF = pd.read_excel('item_details_4k.xlsx')[:10]
# print(class_allocation_func(source_DF))
#################################################################################################################################################################################################################################
