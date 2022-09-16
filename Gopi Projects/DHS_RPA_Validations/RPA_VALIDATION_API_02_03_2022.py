import pandas as pd
import cx_Oracle
# import nltk
from nltk.corpus import words
from spellchecker import SpellChecker
import re
import itertools
from similar_pattern import generate_pattern

# from nltk.corpus import stopwords
# nltk.download('words')
word_list = words.words()


def validation_rpa(record_no):
    con = cx_Oracle.connect("DR1024216/Pipl#mdrm$216@172.16.1.13:1522/DR101411")
    # Not the correct class
    class_data = pd.read_sql('SELECT * FROM DAL_RPA_CLASS_1', con)
    input_data = pd.read_sql('''SELECT RECORD_NO,CLASS_TERM,MASTER_COLUMN6 FROM
                             O_RECORD_MASTER WHERE RECORD_NO='{}' '''.format(record_no), con)

    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Class differs' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    # print(inputData)
    if input_data.shape[0] > 0:
        txt = input_data['MASTER_COLUMN6'][0]
        cls = input_data['CLASS_TERM'][0]
        val_lst_1 = []
        for in_des, in_cls in zip(class_data['SOURCE_DES'], class_data['ACTUAL_CLASS']):
            if (in_des not in txt) and (in_cls != cls):
                val_lst_1.append(True)
            elif (in_des in txt) and (in_cls == cls):
                val_lst_1.append(False)
        result_dict = {}
        # print(val_lst_1)
        if all(val_lst_1):
            result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0]]})

    # Class differs as per dimensions1
    input_data1 = pd.read_sql('''SELECT RECORD_NO,CLASS_TERM,MASTER_COLUMN6 FROM
                                 O_RECORD_MASTER WHERE RECORD_NO='{}' '''.format(record_no), con)
    if input_data1.shape[0] > 0:
        class_data1 = pd.read_sql(
            """SELECT * FROM DAL_RPA_CLASS_5 WHERE CLASS = '{}' """.format(input_data1['CLASS_TERM'][0]), con)

        char_type_val1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                    DAL_RPA_TYPES WHERE ITEM = 'Class differs as per dimentions1' AND FROM_STATUS = 'A1-REGISTERED (AA)'
                    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

        # print(inputData)
        # if class_data1.shape[0] >= 1:
        if input_data1.shape[0] > 0:
            txt = input_data1['MASTER_COLUMN6'][0]
            cls = input_data1['CLASS_TERM'][0]
            val_lst_1 = []
            for qu_key, in_des, in_cls in zip(class_data1['QUALIFIER_KEYWORD'], class_data1['CLASS_KEYWORD'],
                                              class_data1['CLASS']):
                # if (qu_key not in txt) and (in_des not in txt) and (in_cls != cls):
                #     val_lst_1.append(True)
                if (re.search(qu_key, txt)) and (re.search(in_des, txt)) and (in_cls != cls):
                    print(qu_key)
                    val_lst_1.append(True)
            # print('values:', val_lst_1)
            if any(val_lst_1):
                result_dict.update({char_type_val1['SHORT_MESSAGE'][0]: [char_type_val1['MESSAGE'][0]]})

    # Class does not exist in PPO
    input_data2 = pd.read_sql('''SELECT RECORD_NO,CLASS_TERM FROM
                             O_RECORD_MASTER WHERE RECORD_NO='{}' '''.format(record_no), con)

    char_type_val2 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Class does not exist in PPO' AND FROM_STATUS = 'A1-REGISTERED (AA)' AND 
            TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    if input_data2.shape[0] > 0:
        org_terminology = pd.read_sql("""SELECT TERM FROM ORGN_TERMINOLOGY WHERE CONCEPT_TYPE = 'Class' 
                        AND LANGUAGE = 'English US' AND TERM = '{}'  """.format(input_data2['CLASS_TERM'][0]), con)

        if org_terminology.shape[0] == 0:
            result_dict.update({char_type_val2['SHORT_MESSAGE'][0]: [char_type_val2['MESSAGE'][0]]})

    # Incorrect property UOM
    class_data = pd.read_sql("""SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO='{}' AND 
    CLASS_TERM IS NOT NULL""".format(record_no),
                             con)
    char_data = pd.read_sql("""SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_UOM,PROPERTY_VALUE1 FROM O_RECORD_CHAR WHERE 
                            RECORD_NO ='{}' AND PROPERTY_UOM NOT IN('NA','N/A','NOT APPLICABLE')
                            AND PROPERTY_UOM IS NOT NULL""".format(record_no), con)
    # print(char_data)
    if class_data.shape[0] > 0:
        check_data = pd.read_sql("SELECT * FROM DAL_RPA_PROP_6 WHERE CLASS= '{}' ".format(class_data['CLASS_TERM'][0]),
                                 con)

        char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                    DAL_RPA_TYPES WHERE ITEM = 'Incorrect property UOM' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

        char_data1 = char_data.loc[char_data['PROPERTY_NAME'].isin(list(check_data['PROPERTY'])), :].reset_index(
            drop=True)
        char_data1['UOM_CHECK'] = ''
        # print(check_data)
        for i, j in enumerate(char_data1['PROPERTY_UOM']):
            check_data1 = check_data.loc[(check_data['UOM'] == j) | (check_data['RULE_UOM'] == j), :]
            # print(check_data1)
            if check_data1.shape[0] >= 1:
                char_data1['UOM_CHECK'][i] = True
            else:
                char_data1['UOM_CHECK'][i] = False
        unmatched = char_data1.loc[char_data1['UOM_CHECK'] == False,
                                   ['PROPERTY_NAME', 'PROPERTY_VALUE1', 'PROPERTY_UOM']].reset_index(drop=True)
        # print(unmatched)
        # print('....')
        unmatched['new_values'] = unmatched['PROPERTY_VALUE1'].str.cat(unmatched['PROPERTY_UOM'], sep=',')
        unmatched['PROPERTY_VALUE1'] = unmatched['new_values'].apply(lambda prp_value: str((prp_value,)))
        unmatched['values'] = unmatched['PROPERTY_NAME'] + unmatched['PROPERTY_VALUE1']
        unmatched = ','.join(list(unmatched['values']))
        # print(char_data1['UOM_CHECK'])
        if not all(list(char_data1['UOM_CHECK'])):
            result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0], unmatched]})

    # Class differs as per dimensions
    prop_4 = pd.read_sql("""SELECT * FROM DAL_RPA_CLASS_4 WHERE PROPERTY IN(SELECT PROPERTY_NAME FROM O_RECORD_CHAR 
                                WHERE RECORD_NO = '{}')""".format(record_no), con)
    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Class differs as per dimentions' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    bool_values = []
    # print(len(bool_values))
    # result_dict = {}
    prop_4['CLASS'] = prop_4['CLASS'].apply(lambda cla: re.sub(r'\*', '', cla))
    if class_data.shape[0] >= 1:
        class_presence = [i for i in prop_4['CLASS'] if i in class_data['CLASS_TERM'][0]]
        if len(class_presence) > 0:
            # for i in char_data['PROPERTY_NAME']:
            propert_value = prop_4[prop_4['CLASS'].isin(class_presence)].reset_index(drop=True)
            char_data1 = char_data.loc[char_data['PROPERTY_NAME'].isin(propert_value['PROPERTY'])].reset_index(
                drop=True)
            if char_data1.shape[0] >= 1:
                char_data1['PROPERTY_VALUE2'] = char_data1.apply(lambda uom: float(eval(uom['PROPERTY_VALUE1'])) * 25.5
                if uom['PROPERTY_UOM'] == 'IN' else uom['PROPERTY_VALUE1'], axis=1)
                print(char_data1)
                if propert_value.shape[0] > 0:
                    propert_value = propert_value[['VALUE', 'CLASS']]
                    for k, j in zip(propert_value['VALUE'], propert_value['CLASS']):
                        # print(char_data1)
                        expre = ''.join(re.sub(r'[a-zA-Z\d]', '', str(k)))
                        number = '{}'.format(''.join(re.findall(r'\d', str(k))))
                        # print(number, expre)
                        final_check = eval(f"int(char_data1['PROPERTY_VALUE2'][0]){expre}{number}")
                        # print(final_check)
                        if not final_check:
                            bool_values.append(char_data1['PROPERTY_NAME'][0] + str((char_data1['PROPERTY_VALUE1'][0],
                                                                                     char_data1['PROPERTY_UOM'][0])))
                            break
                        # print(expre)

                else:
                    pass
        # print(bool_values)
        bool_values1 = ','.join(bool_values)
        if len(bool_values) > 0:
            result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0], bool_values1]})
    # Same property value assigned more than once
    property_value = pd.read_sql(
        "SELECT RECORD_NO,PROPERTY_VALUE1,PROPERTY_NAME FROM O_RECORD_CHAR WHERE RECORD_NO = '{}' ".format(record_no),
        con)
    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                            DAL_RPA_TYPES WHERE ITEM = 'Same property value assigned more than once' AND 
                            FROM_STATUS = 'A1-REGISTERED (AA)'AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    property_value.dropna(how='any', inplace=True)
    # print([list(property_value['PROPERTY_VALUE1']).count(i) for i in property_value['PROPERTY_VALUE1']])
    values = [j + str((i,)) for i, j in zip(property_value['PROPERTY_VALUE1'], property_value['PROPERTY_NAME']) if
              list(property_value['PROPERTY_VALUE1']).count(i) >= 2]
    values = ','.join(list(values))
    if len(values) > 0:
        result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0], values]})

    # Unwanted special characters and spaces
    property_value = pd.read_sql(
        "SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1 FROM O_RECORD_CHAR WHERE RECORD_NO = '{}' ".format(record_no),
        con)
    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Unwanted special charecters and spaces' AND FROM_STATUS = 'A1-REGISTERED (AA)'
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    property_value.fillna('', inplace=True, )
    property_value['special_char'] = [True if not (re.search(r'^[-+/,]{2,}|[^\-+\w\s/,]|\s{2,}', j)) else False
                                      for j in property_value['PROPERTY_VALUE1']]
    value_check = property_value.loc[property_value['special_char'] == False, ['PROPERTY_NAME', 'PROPERTY_VALUE1']]
    value_check['PROPERTY_VALUE1'] = value_check['PROPERTY_VALUE1'].apply(lambda prp_value: str((prp_value,)))
    value_check1 = value_check['PROPERTY_NAME'] + value_check['PROPERTY_VALUE1']
    value_check1 = ','.join(list(value_check1))
    # print(value_check)
    if not all(list(property_value['special_char'])):
        result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0], value_check1]})

    # Data type (Number/String) miss match
    char_data = pd.read_sql(
        "SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1 FROM O_RECORD_CHAR WHERE RECORD_NO='{}' ".format(record_no),
        con)
    class_term = pd.read_sql(
        "SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' ".format(record_no), con)
    if class_term.shape[0] > 0:
        datatype = pd.read_sql("""SELECT DISTINCT PROPERTY_TERM,DATA_TYPE,CLASS_TERM FROM ORGN_DR 
                                WHERE DOMAIN = 'PRODUCT' AND LOCALE = 'en_US' AND CLASS_TERM = '{}' """.format(
            class_term['CLASS_TERM'][0]), con)
        dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Data type (Number/String) miss match' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

        char_data.dropna(axis=0, inplace=True, how='any')
        datatype = datatype.loc[datatype['PROPERTY_TERM'].isin(list(char_data['PROPERTY_NAME'])), :]
        char_data['DATA_TYPE'] = ['STRING' if not i.isnumeric() else 'MEASURE_NUMBER_TYPE' for i in
                                  char_data['PROPERTY_VALUE1']]
        values = []
        for i, j in zip(char_data['PROPERTY_NAME'], char_data['PROPERTY_VALUE1']):
            value_check = char_data.loc[char_data['PROPERTY_NAME'] == i, ['DATA_TYPE']].reset_index(drop=True)
            value_check1 = datatype.loc[datatype['PROPERTY_TERM'] == i, ['DATA_TYPE']].reset_index(drop=True)

            if value_check['DATA_TYPE'][0] != value_check1['DATA_TYPE'][0]:
                values.append(i + str((j,)))
        values = ','.join(values)

        if set(char_data['DATA_TYPE']) != set(datatype['DATA_TYPE']):
            result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], values]})

    # Same Part No and diff classes
    class_data = pd.read_sql(
        """SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' """.format(record_no), con)
    ref_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE 
                            WHERE REFERENCE_TYPE IN ('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') 
                            AND (RECORD_NO = '{}') AND REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Same Part No and diff classes' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    # print('....')
    reference_part = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE 
                            WHERE RECORD_NO != '{0}' AND REFERENCE_NO IN (SELECT REFERENCE_NO FROM O_RECORD_REFERENCE 
                            WHERE REFERENCE_TYPE IN ('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') 
                            AND (RECORD_NO = '{0}') AND REFERENCE_NO != 'UNKNOWN') AND VENDOR_NAME IN (SELECT 
                            VENDOR_NAME FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE IN ('OEM PART NO','MANUFACTURER 
                            PART NO','SUPPLIER PART NO') AND RECORD_NO = '{0}' AND VENDOR_NAME != 'UNKNOWN')  """.format(
        record_no), con)

    # if reference_part.shape[0] >= 1:
    #     ref_data1 = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE
    #         REFERENCE_NO='{}' AND RECORD_NO != '{}' """.format(reference_part['REFERENCE_NO'][0], record_no), con)
    # print(reference_part)
    if reference_part.shape[0] >= 1:
        class_data1 = pd.read_sql("""SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE 
                                                 RECORD_NO='{}' """.format(reference_part['RECORD_NO'][0]), con)

        # clas_values = [True if class_data['CLASS_TERM'][0] != i else False for i in class_data1['CLASS_TERM']]
        # print(clas_values)
        if class_data1['CLASS_TERM'][0] != class_data['CLASS_TERM'][0]:
            ref_values = list(set(reference_part['REFERENCE_NO']))
            ref_values.append(str((class_data1['CLASS_TERM'][0], class_data['CLASS_TERM'][0])))
            ref_values1 = ' '.join(ref_values)
            result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], ref_values1]})
    # print('time taking')
    # Unwanted special chars in Part No
    ref_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
            IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO= '{}' """.format(record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Unwanted special chars in Part No' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    value_check = [True if not re.search(r"[,\-_+*/.<>]{2,}|\s{2,}|[^,\-_+*/.< >\w]", i) else False for i
                   in ref_data['REFERENCE_NO']]
    ref_data['value_check'] = value_check
    # print(ref_data['REFERENCE_NO'])
    # print(ref_data)
    if not all(value_check):
        ref_data1 = ref_data.loc[ref_data['value_check'] == False, :]
        # print(ref_data1)
        ref_data1['REFERENCE_NO'] = ref_data1['REFERENCE_NO'].apply(lambda ref_value: str((ref_value,)))
        ref_data1['display_values'] = ref_data1['REFERENCE_TYPE'] + ' ' + ref_data1['REFERENCE_NO']
        # print(ref_data1['display_values'])
        ref_values1 = ' '.join(list(ref_data1['display_values']))
        result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], ref_values1]})

    # Miss spelt property value
    property_words = pd.read_sql("SELECT ABBREVIATION FROM DAL_RPA_PROP_10", con)
    property_value = pd.read_sql(
        "SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1 FROM O_RECORD_CHAR WHERE RECORD_NO = '{}' ".format(record_no),
        con)
    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Miss spelt property value' AND FROM_STATUS = 'A1-REGISTERED (AA)'
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    property_words.dropna(how='any', inplace=True)
    spell = SpellChecker()
    spell.word_frequency.load_words(list(property_words['ABBREVIATION']))
    spell.word_frequency.load_words(word_list)
    property_value.dropna(axis=0, how='any', inplace=True)
    property_value.drop(property_value[property_value['PROPERTY_NAME'] == 'MATERIAL'].index, inplace=True)
    property_value['PROPERTY_VALUE1'] = property_value['PROPERTY_VALUE1'].apply(lambda x: re.sub(r'\W', ' ', x))
    values = [text.split() for text in property_value['PROPERTY_VALUE1'] if len(text) > 2]
    values = list(itertools.chain(*values))
    values = [i for i in values if
              (not re.search(r'[^a-zA-z\s]', i)) and (i not in list(property_words['ABBREVIATION']))]
    # print(values)
    misspelled = spell.unknown(values)
    # print(misspelled)
    # values1=[spell.correction(text) for text in misspelled ]
    # values1=[i for i in values1 if i not in stopwords.words('english')]

    values2 = ','.join(list(misspelled))
    if len(misspelled) >= 1:
        result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0], values2]})

    #  Same Mnfr with  diff Part No Types
    vendor_data = pd.read_sql("""SELECT VENDOR_NAME,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
        IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' 
        AND VENDOR_NAME != 'UNKNOWN' """.format(record_no), con)
    vendor_data1 = pd.read_sql("""SELECT VENDOR_NAME,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
                                IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') 
                                AND VENDOR_NAME IN (SELECT VENDOR_NAME FROM O_RECORD_REFERENCE WHERE
                                REFERENCE_TYPE IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND 
                                RECORD_NO='{}' AND VENDOR_NAME != 'UNKNOWN')""".format(record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Same Mnfr with  diff Part No Types' AND FROM_STATUS = 'A1-REGISTERED (AA)'
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    # print(vendor_data1)
    values = []
    for i in vendor_data1['VENDOR_NAME'].unique():
        vendor_types = vendor_data1.loc[vendor_data1['VENDOR_NAME'] == i, :]
        if len(vendor_types['REFERENCE_TYPE'].unique()) > 1:
            values.append(i)
            values.append(str(tuple(vendor_types['REFERENCE_TYPE'].unique())))
    # print(vendor_data['VENDOR_NAME'])
    values1 = ' '.join(values)
    if len(values) >= 1:
        result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], values1]})

    # Unwanted special chars in Reference No
    part_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE NOT
             IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO= '{}' """.format(record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
             DAL_RPA_TYPES WHERE ITEM = 'Unwanted special chars in Reference No' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
             AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    value_check1 = [True if not re.search(r"[,\-_+*/.<>]{2,}|\s{2,}|[^,\-_+*/.< >\w]", i) else False for i
                    in part_data['REFERENCE_NO']]
    part_data['value_check'] = value_check1
    # print(ref_data['REFERENCE_NO'])
    # print(value_check)
    if not all(value_check1):
        ref_data2 = part_data.loc[part_data['value_check'] == False, :]
        # print(ref_data1)
        ref_data2['REFERENCE_NO'] = ref_data2['REFERENCE_NO'].apply(lambda ref_value: str((ref_value,)))
        ref_data2['display_values'] = ref_data2['REFERENCE_TYPE'] + ' ' + ref_data2['REFERENCE_NO']
        # print(ref_data1['display_values'])
        ref_values2 = ' '.join(list(ref_data2['display_values']))
        result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], ref_values2]})
    # Unwanted spaces in Manufacturer name
    vendor_data = pd.read_sql(
        "SELECT RECORD_NO,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE RECORD_NO = '{}' ".format(record_no), con)
    dal_types1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Unwanted spaces in Manufacturer name' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    value_check1 = [i for i in vendor_data['VENDOR_NAME'] if re.search(r"\s{2,}", i)]
    value_check2 = ' '.join(value_check1)
    if len(value_check1) > 0:
        result_dict.update({dal_types1['SHORT_MESSAGE'][0]: [dal_types1['MESSAGE'][0], value_check2]})

    # Unwanted spaces in Doc No
    document_data = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_NO,REVISION,DOCUMENT_ITEM,ITEM_POSITION FROM 
                                    O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' """.format(record_no), con)
    dal_types2 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Unwanted spaces in Doc No' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    value_check3 = [i for i in document_data['DOCUMENT_NO'] if re.search(r"\s{2,}", i)]
    value_check4 = ' '.join(value_check3)
    if len(value_check3) > 0:
        result_dict.update({dal_types2['SHORT_MESSAGE'][0]: [dal_types2['MESSAGE'][0], value_check4]})

    # Unwanted spaces in Doc Rev No
    dal_types3 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Unwanted spaces in Doc Rev No' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    value_check5 = [i for i in document_data['REVISION'] if re.search(r"\s{2,}", i)]
    value_check6 = ' '.join(value_check5)
    if len(value_check5) > 0:
        result_dict.update({dal_types3['SHORT_MESSAGE'][0]: [dal_types3['MESSAGE'][0], value_check6]})

    # Unwanted spaces in Doc Item No
    dal_types4 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
                DAL_RPA_TYPES WHERE ITEM = 'Unwanted spaces in Doc Item No' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
                AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    value_check7 = [i for i in document_data['DOCUMENT_ITEM'] if re.search(r"\s{2,}", str(i))]
    value_check8 = ' '.join(value_check7)
    if len(value_check7) > 0:
        result_dict.update({dal_types4['SHORT_MESSAGE'][0]: [dal_types4['MESSAGE'][0], value_check8]})

    # Doc No captured as Part No aswell
    doc_data = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_NO AS REFERENCE_NO,DOCUMENT_TYPE AS REFERENCE_TYPE FROM 
    O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' AND DOCUMENT_NO != 'UNKNOWN' """.format(record_no), con)
    ref_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
        IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' 
        AND REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Doc No captured as Part No aswell' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    part_doc_data = pd.concat([doc_data, ref_data], ignore_index=True)
    # ref_data1 = ref_data.loc[ref_data['REFERENCE_NO'].isin(list(doc_data['DOCUMENT_NO'])),
    #                          ['REFERENCE_NO', 'REFERENCE_TYPE']]
    ref_data1 = part_doc_data[part_doc_data.duplicated(subset=['REFERENCE_NO'], keep=False)]
    ref_data1['REFERENCE_NO'] = ref_data1['REFERENCE_NO'].apply(lambda reno: str((reno,)))
    ref_data1['display_values'] = ref_data1['REFERENCE_TYPE'] + ' ' + ref_data1['REFERENCE_NO']
    # print(ref_data)
    values = ','.join(list(ref_data1['display_values']))
    if ref_data1.shape[0] >= 1:
        result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], values]})
    # else:
    #     result_dict.update({dal_types['SHORT_MESSAGE'][0]: ["Success"]})

    # Doc No captured as Ref No as well
    doc_data1 = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_NO AS REFERENCE_NO, DOCUMENT_TYPE AS REFERENCE_TYPE FROM 
    O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' AND DOCUMENT_NO != 'UNKNOWN' """.format(record_no), con)
    ref_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
        NOT IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' AND 
        REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    dal_types1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Doc No captured as Ref No aswell' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    ref_doc_data = pd.concat([doc_data1, ref_data], ignore_index=True)
    # ref_data2 = ref_data.loc[ref_data['REFERENCE_NO'].isin(list(doc_data1['DOCUMENT_NO'])),
    #                          ['REFERENCE_NO', 'REFERENCE_TYPE']]
    ref_data2 = ref_doc_data[ref_doc_data.duplicated(subset=['REFERENCE_NO'], keep=False)]
    ref_data2['REFERENCE_NO'] = ref_data2['REFERENCE_NO'].apply(lambda reno: str((reno,)))
    ref_data2['display_values'] = ref_data2['REFERENCE_TYPE'] + ' ' + ref_data2['REFERENCE_NO']
    # print(ref_data)
    values1 = ','.join(list(ref_data2['display_values']))
    if ref_data2.shape[0] >= 1:
        # print('doc ref')
        # print(ref_data2)
        result_dict.update({dal_types1['SHORT_MESSAGE'][0]: [dal_types1['MESSAGE'][0], values1]})

    # Same Doc No with Diff revision No
    doc_data = pd.read_sql(
        "SELECT RECORD_NO,DOCUMENT_NO,REVISION FROM O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' ".format(record_no), con)

    doc_data1 = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_NO,REVISION FROM O_RECORD_DOCUMENT 
                        WHERE DOCUMENT_NO IN (SELECT DOCUMENT_NO FROM O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' ) 
                        """.format(record_no, record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM = 'Same Doc No with Diff revision No' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    # doc_data2 = doc_data1.loc[~doc_data1['REVISION'].isin(list(doc_data['REVISION'])), :]
    values = []
    for k in doc_data1['DOCUMENT_NO'].unique():
        doc_data2 = doc_data1.loc[doc_data1['DOCUMENT_NO'] == k, :]
        if len(doc_data2['REVISION'].unique()) > 1:
            values.append(k + ' ' + str(tuple(doc_data2['REVISION'].unique())))
    # print(doc_data2)
    values1 = ','.join(values)
    if len(values) > 0:
        # doc_data1['REVISION'] = doc_data1['REVISION'].apply(lambda rev: str((rev,)))
        # doc_data1['display_values'] = doc_data1['DOCUMENT_NO'] + ' ' + doc_data1['REVISION']
        # values = ','.join(list(set(doc_data1['display_values'])))
        result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], values1]})

    # Same Part/ref assigned more than once
    part_no = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
                            IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' AND 
                            REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    reference_no = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE 
    WHERE REFERENCE_TYPE NOT IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' 
    AND REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    dal_types2 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Same Part/ref assigned more than once' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    part_check = pd.concat([reference_no, part_no], ignore_index=True)
    part_check_group = part_check[part_check.duplicated(subset=['REFERENCE_NO'], keep=False)]
    part_check_duplicates = part_check_group.drop_duplicates(subset=['REFERENCE_TYPE'], keep='first')
    part_check_duplicates['REFERENCE_NO'] = part_check_duplicates['REFERENCE_NO'].apply(lambda num: str((num,)))
    part_check_duplicates['display_values'] = part_check_duplicates['REFERENCE_TYPE'] + ' ' + part_check_duplicates[
        'REFERENCE_NO']
    values2 = ','.join(list(part_check_duplicates['display_values']))
    if part_check_duplicates.shape[0] >= 1:
        result_dict.update({dal_types2['SHORT_MESSAGE'][0]: [dal_types2['MESSAGE'][0], values2]})

    # Manufacturer spelt incorrectly
    ref_data = pd.read_sql("""SELECT RECORD_NO,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE 
                            RECORD_NO = '{}' """.format(record_no), con)
    char_type_val1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM = 'Manufacturer spelt incorrcetly' AND FROM_STATUS = 'A1-REGISTERED (AA)'
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    ref_data['VENDOR_NAME'] = ref_data['VENDOR_NAME'].apply(lambda test: re.sub(r'\W', ' ', test))
    values1 = [text.split() for text in ref_data['VENDOR_NAME'] if len(text) > 2]
    values1 = list(itertools.chain(*values1))
    misspelled1 = spell.unknown(values1)
    display_values = ','.join(misspelled1)
    if len(misspelled1) >= 1:
        result_dict.update({char_type_val1['SHORT_MESSAGE'][0]: [char_type_val1['MESSAGE'][0], display_values]})

    # Doc Pos/Item Number
    dal_types4 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM = 'Doc Pos/Item Number' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    doc_pos = part_no.loc[(part_no['REFERENCE_NO'].isin(list(document_data['ITEM_POSITION']))) |
                          (part_no['REFERENCE_NO'].isin(list(document_data['DOCUMENT_ITEM']))) |
                          (part_no['REFERENCE_NO'].isin(list(document_data['DOCUMENT_NO']))), ['REFERENCE_NO']]
    # print(doc_pos)
    values_ref = ','.join(list(doc_pos['REFERENCE_NO']))
    if doc_pos.shape[0] >= 1:
        result_dict.update({dal_types4['SHORT_MESSAGE'][0]: [dal_types4['MESSAGE'][0], values_ref]})

    # Doc/Item/Pos Numbers not captured properly
    original_text = pd.read_sql(
        "SELECT RECORD_NO,MASTER_COLUMN6 FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' ".format(record_no), con)
    dal_types3 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
     DAL_RPA_TYPES WHERE ITEM = 'Doc/Item/Pos Numbers not captured properly' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
     AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    doc_values = []
    document_data.fillna('', axis=0, inplace=True)
    document_data.replace('0', ' ', inplace=True)
    document_data.replace('UNKNOWN', '', inplace=True)
    # print(original_text['MASTER_COLUMN6'][0])
    for x, y, z in zip(document_data['DOCUMENT_NO'], document_data['REVISION'], document_data['ITEM_POSITION']):
        if (x.strip() in original_text['MASTER_COLUMN6'][0]) and (
                re.search(r'\b{}\b'.format(y.strip()), original_text['MASTER_COLUMN6'][0])) and (
                re.search(f'{z.strip()}', original_text['MASTER_COLUMN6'][0])):
            doc_values.append(True)
        else:
            doc_values.append(False)
    document_data['value_check'] = doc_values
    wrong_values = document_data.loc[document_data['value_check'] == False, ['DOCUMENT_NO']]
    wrong_values1 = document_data.loc[(document_data['DOCUMENT_NO'].isin(list(document_data['REVISION']))) | (
        document_data['DOCUMENT_NO'].isin(list(document_data['ITEM_POSITION']))), ['DOCUMENT_NO']]
    wrong_values2 = document_data.loc[
        document_data['REVISION'].isin(list(document_data['ITEM_POSITION'])), ['DOCUMENT_NO']]
    values3 = [list(wrong_values['DOCUMENT_NO']), list(wrong_values1['DOCUMENT_NO']),
               list(wrong_values2['DOCUMENT_NO'])]
    # print(values3)
    values3 = ','.join(list(itertools.chain(*values3)))

    df_shapes = [wrong_values.shape[0], wrong_values1.shape[0], wrong_values2.shape[0]]
    # print(df_shapes)
    if any(df_shapes) >= 1:
        result_dict.update({dal_types3['SHORT_MESSAGE'][0]: [dal_types3['MESSAGE'][0], values3]})

    # Non-standard property value
    property_value = pd.read_sql("""SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1,PROPERTY_UOM FROM O_RECORD_CHAR WHERE 
                                    PROPERTY_VALUE1 IS NOT NULL AND RECORD_NO = '{}' """.format(record_no), con)
    class_data = pd.read_sql("""SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' 
    AND CLASS_TERM IS NOT NULL """.format(record_no), con)
    prop_9 = pd.read_sql("""SELECT * FROM DAL_RPA_PROP_9 WHERE CLASS IN 
    (SELECT CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}') """.format(record_no), con)
    if class_data.shape[0] > 0:

        char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM = 'Non-standard property value ' AND FROM_STATUS = 'A1-REGISTERED (AA)'
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
        # print(char_type_val)
        not_matched = property_value.loc[~((property_value['PROPERTY_NAME'].isin(list(prop_9['PROPERTY']))) &
                                           (property_value['PROPERTY_VALUE1'].isin(list(prop_9['VALUE'])))), [
                                             'PROPERTY_NAME', 'PROPERTY_VALUE1']]
        not_matched['PROPERTY_VALUE1'] = not_matched['PROPERTY_VALUE1'].apply(lambda val: str((val,)))
        # print(not_matched)
        # print('....')
        display_values = not_matched['PROPERTY_NAME'] + not_matched['PROPERTY_VALUE1']

        display_values = ','.join(list(display_values))

        if not_matched.shape[0] >= 1:
            # print(display_values)
            # print('....')
            result_dict.update({char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0], display_values]})

    # Mnfr name in FFT
    vendor_data = pd.read_sql(
        "SELECT RECORD_NO,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE RECORD_NO = '{}' ".format(record_no), con)
    fft_text = pd.read_sql(
        "SELECT RECORD_NO,MASTER_COLUMN10,MASTER_COLUMN6,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' ".format(
            record_no), con)
    dal_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM = 'Mnfr name in FFT' AND FROM_STATUS = 'A1-REGISTERED (AA)'
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    # print(fft_text)
    values = [i for i in vendor_data['VENDOR_NAME'] if
              re.search(r'\b{}\b'.format(str(i)), str(fft_text['MASTER_COLUMN10'][0]))]
    display_values1 = ','.join(values)

    if len(values) >= 1:
        result_dict.update({dal_types['SHORT_MESSAGE'][0]: [dal_types['MESSAGE'][0], display_values1]})
    part_no = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
                            IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' 
                            AND REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    reference_no = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE 
    REFERENCE_TYPE NOT IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' AND 
    REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    dal_types1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM = 'Ref Number in FFT' AND FROM_STATUS = 'A1-REGISTERED (AA)'
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    dal_types2 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM IN('Part Number in FFT','Descriptor in FFT') AND FROM_STATUS = 'A1-REGISTERED (AA)'
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    values1 = [j + ' ' + str((i,)) for j, i in zip(part_no['REFERENCE_TYPE'], part_no['REFERENCE_NO']) if
               re.search(r'\b(^-|:|:-|,|\s)(\s|)(\s|){}\b'.format(str(i)), str(fft_text['MASTER_COLUMN10'][0]))]
    values2 = [k + ' ' + str((i,)) for k, i in zip(reference_no['REFERENCE_TYPE'], reference_no['REFERENCE_NO']) if
               re.search(r'\b(^-|:|:-|,|\s)(\s|){}\b'.format(str(i)), str(fft_text['MASTER_COLUMN10'][0]))]
    # print(values1,values2)
    display_values2 = ','.join(values1)
    display_values3 = ','.join(values2)

    if len(values2) >= 1:
        result_dict.update({dal_types1['SHORT_MESSAGE'][0]: [dal_types1['MESSAGE'][0], display_values3]})
    if len(values1) >= 1:
        # print('...')
        result_dict.update({dal_types2['SHORT_MESSAGE'][1]: [dal_types2['MESSAGE'][1], display_values2]})

    if (class_data.shape[0] >= 1) and (re.search(r'\b(^-|:|:-|,|\s)(\s|){}\b'.format(str(class_data['CLASS_TERM'][0])),
                                                 str(fft_text['MASTER_COLUMN10'][0]))):
        result_dict.update(
            {dal_types2['SHORT_MESSAGE'][0]: [dal_types2['MESSAGE'][0], str(class_data['CLASS_TERM'][0])]})
    # print(dal_types2)
    document_data = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_NO,DOCUMENT_TYPE,REVISION,DOCUMENT_ITEM,ITEM_POSITION 
        FROM O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' """.format(record_no), con)
    document_message = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM IN('Document Number in  FFT') AND FROM_STATUS = 'A1-REGISTERED (AA)' 
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    property_message = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM IN('Property values appears in FFT') AND FROM_STATUS = 'A1-REGISTERED (AA)' 
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    unwanted_message = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM IN('Unwanted spc/spl chars in FFT') AND FROM_STATUS = 'A1-REGISTERED (AA)' 
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    refe_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE
                         RECORD_NO='{}' AND REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    message_data = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM = 'Missing data not captured in FFT' AND FROM_STATUS = 'A1-REGISTERED (AA)'
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    # con.close()
    document_data.fillna('', inplace=True)
    values3 = [i for i in document_data['DOCUMENT_NO'] if
               re.search(r'\b(^-|:|:-|,|\s)(\s|){}\b'.format(str(i)), str(fft_text['MASTER_COLUMN10'][0]))]
    display_values4 = ','.join(values3)
    # print(dal_types3)
    if len(values3) >= 1:
        result_dict.update({document_message['SHORT_MESSAGE'][0]: [document_message['MESSAGE'][0], display_values4]})
    values4 = [J + ' ' + str((i,)) for i, J in zip(property_value['PROPERTY_VALUE1'], property_value['PROPERTY_NAME'])
               if
               re.search(r'\b(^-|:|:-|,|\s)(\s|){}\b'.format(str(i)), str(fft_text['MASTER_COLUMN10'][0]))]
    display_values5 = ','.join(values4)
    if len(values4) >= 1:
        result_dict.update({property_message['SHORT_MESSAGE'][0]: [property_message['MESSAGE'][0], display_values5]})
    if re.search(r"[,\-_+*/.<>]{2,}|\s{2,}|[^,\-_+*/.< >\w]", str(fft_text['MASTER_COLUMN10'][0])):
        result_dict.update({unwanted_message['SHORT_MESSAGE'][0]: [unwanted_message['MESSAGE'][0]]})

    # print(remaining_text)
    prop_10 = pd.read_sql("""SELECT * FROM DAL_RPA_PROP_10""", con)
    dal_rpa_types = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM = 'Non-approved property value abbreviation' AND FROM_STATUS = 'A1-REGISTERED (AA)'
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    property_value.dropna(axis=0, how='any', inplace=True)
    prop_values = [i for i in property_value['PROPERTY_VALUE1'] if not re.sub(r'\W', '', i).isnumeric()]
    # print(prop_values)
    # prop_10_new = prop_10.loc[prop_10['VALUE'].isin(prop_values),:]
    # print(prop_10_new)
    abbr_values = []
    for value in prop_values:
        if (value in set(prop_10['VALUE'])) | (value in set(prop_10['ABBREVIATION'])):
            abbr_values.append(True)
        elif (value not in set(prop_10['VALUE'])) | (value not in set(prop_10['ABBREVIATION'])):
            pass
        # else:
        #     pass
    # abbr_values = [True if ((prop_10['VALUE'] == value) | (prop_10['ABBREVIATION'] == value)) else False
    #                for value in prop_values]
    # print(abbr_values)

    # if (len(prop_values) >= 0) & (len(abbr_values) == 0):
    #     result_dict.update({dal_rpa_types['SHORT_MESSAGE'][0]: [dal_rpa_types['MESSAGE'][0]]})
    dal_prop_8 = pd.read_sql("SELECT * FROM DAL_RPA_PROP_8", con)
    dal_rpa_types1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
    DAL_RPA_TYPES WHERE ITEM = 'Incorrcet property UOM conversion' AND FROM_STATUS = 'A1-REGISTERED (AA)'
    AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    # message_data1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM
    # DAL_RPA_TYPES WHERE ITEM = 'Class differs as per dimentions1' AND FROM_STATUS = 'A1-REGISTERED (AA)'
    # AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    # prop_5 = pd.read_sql("SELECT * FROM DAL_RPA_CLASS_5", con)
    # qualifier_keywords = []
    # for i, j in zip(prop_5['QUALIFIER_KEYWORD'], prop_5['CLASS']):
    #     if (re.search(i, fft_text['MASTER_COLUMN6'][0])) and (j != fft_text['CLASS_TERM'][0]):
    #         qualifier_keywords.append(True)
    #         break
    # # print(qualifier_keywords)
    # if len(qualifier_keywords) >= 1:
    #     result_dict.update({message_data1['SHORT_MESSAGE'][0]: message_data1['MESSAGE'][0]})

    # con.close()
    # print(fft_text['MASTER_COLUMN6'][0])
    # print(re.search(r'\b\s{}\b'.format(str('5/16')),str(fft_text['MASTER_COLUMN6'][0])))
    uom_values = [j for i, j in zip(dal_prop_8['FROM_VALUE'], dal_prop_8['TO_VALUE']) if
                  re.search(r'\b\s{0}\"|\bX{0}\"|\b\s{0}\s\b'.format(str(re.escape(j))),
                            str(fft_text['MASTER_COLUMN6'][0]))]
    uom_type = [i for i in dal_prop_8['TO_UOM'] if re.search(r'\b{}\b'.format(i), str(fft_text['MASTER_COLUMN6'][0]))]
    # print(uom_values)
    dal_prop_new = dal_prop_8.loc[(dal_prop_8['TO_VALUE'].isin(uom_values)) &
                                  (dal_prop_8['TO_UOM'].isin(uom_type)), :]
    # print(dal_prop_new)
    # print('////')
    uom_conversion = property_value.loc[
                     property_value['PROPERTY_VALUE1'].isin(dal_prop_new['FROM_VALUE']) | property_value[
                         'PROPERTY_VALUE1'].isin(dal_prop_new['TO_VALUE']), :]
    # print(uom_conversion)
    # print(',,,,,')
    if (len(uom_values) != 0) & (uom_conversion.shape[0] != dal_prop_new.shape[0]):
        result_dict.update({dal_rpa_types1['SHORT_MESSAGE'][0]: [dal_rpa_types1['MESSAGE'][0]]})

    # similar_validations
    reference_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE 
    WHERE REFERENCE_NO != 'UNKNOWN' AND REFERENCE_TYPE IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND 
    VENDOR_NAME != 'UNKNOWN' """, con)
    dal_rpa_types1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM ='Similar Part Nos. and diff Mnfr Names' AND FROM_STATUS = 'A1-REGISTERED (AA)' AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''',
                                 con)
    dal_rpa_types2 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM ='Similar ref No with diff Mnfr' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    dal_rpa_types3 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM ='Similar Ref No with diff labels' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    dal_rpa_types4 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM ='Similar Part No and  diff Part No Types' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    dal_rpa_types5 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM ='Similar Doc No with Diff Doc Type label' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    part_no1 = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE REFERENCE_TYPE 
                            IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' 
                            AND REFERENCE_NO != 'UNKNOWN' AND VENDOR_NAME != 'UNKNOWN' """.format(record_no), con)

    # global part_no

    def vendor_name(x, y, name1, name2):
        # global x
        copy_data = x
        copy_data1 = y
        regex_patterns = [generate_pattern(i) for i in copy_data[name2]]
        # print()
        values = []
        value1 = []
        for k, i in enumerate(regex_patterns):
            # print(part_no['VENDOR_NAME'][k])
            matched_values = [j for j in copy_data1[name2] if re.search(r'^{}$'.format(i), j)]
            # print(matched_values)
            if len(matched_values) >= 1:
                # print(matched_values)
                reference_data1 = copy_data1[(copy_data1[name2].isin(matched_values))]
                print(reference_data1)
                reference_type = reference_data1.loc[reference_data1[name1] != copy_data[name1][k]]
                print(reference_type)
                if reference_type.shape[0] >= 1:
                    values.append(copy_data[name2][k])
                    values.append(str(tuple(reference_data1[name1].unique())))
        return values

    # result_dict = {}

    # print(dal_rpa_types1)
    # print(part_no)
    partno_values = vendor_name(part_no1, reference_data, 'VENDOR_NAME', 'REFERENCE_NO')
    print(partno_values)
    # print(',,,,')
    values1 = ' '.join(partno_values)
    if len(partno_values) >= 1:
        result_dict.update({dal_rpa_types1['SHORT_MESSAGE'][0]: [dal_rpa_types1['MESSAGE'][0], values1]})
    reference_no = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE
     WHERE REFERENCE_TYPE NOT IN('OEM PART NO','MANUFACTURER PART NO','SUPPLIER PART NO') AND RECORD_NO='{}' AND 
     REFERENCE_NO != 'UNKNOWN' AND VENDOR_NAME != 'UNKNOWN' """.format(record_no), con)
    reference_data1 = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE,VENDOR_NAME FROM O_RECORD_REFERENCE 
        WHERE REFERENCE_NO != 'UNKNOWN' AND REFERENCE_TYPE NOT IN('OEM PART NO','MANUFACTURER PART NO',
        'SUPPLIER PART NO') AND VENDOR_NAME != 'UNKNOWN' """, con)
    document_data = pd.read_sql(
        "SELECT RECORD_NO,DOCUMENT_NO,DOCUMENT_TYPE FROM O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' ".format(record_no),
        con)
    document_data1 = pd.read_sql("SELECT RECORD_NO,DOCUMENT_NO,DOCUMENT_TYPE FROM O_RECORD_DOCUMENT ", con)
    # con.close()
    reference_values = vendor_name(reference_no, reference_data1, 'VENDOR_NAME', 'REFERENCE_NO')
    values2 = ' '.join(reference_values)
    if len(reference_values) >= 1:
        result_dict.update({dal_rpa_types2['SHORT_MESSAGE'][0]: [dal_rpa_types2['MESSAGE'][0], values2]})
    reference_values1 = vendor_name(reference_no, reference_data1, 'REFERENCE_TYPE', 'REFERENCE_NO')
    values3 = ' '.join(reference_values1)
    if len(reference_values1) >= 1:
        result_dict.update({dal_rpa_types3['SHORT_MESSAGE'][0]: [dal_rpa_types3['MESSAGE'][0], values3]})
    partno_values1 = vendor_name(part_no, reference_data, 'REFERENCE_TYPE', 'REFERENCE_NO')
    values4 = ' '.join(partno_values1)
    if len(partno_values1) >= 1:
        result_dict.update({dal_rpa_types4['SHORT_MESSAGE'][0]: [dal_rpa_types4['MESSAGE'][0], values4]})
    document_values1 = vendor_name(document_data, document_data1, 'DOCUMENT_TYPE', 'DOCUMENT_NO')
    values5 = ' '.join(document_values1)
    if len(document_values1) >= 1:
        result_dict.update({dal_rpa_types5['SHORT_MESSAGE'][0]: [dal_rpa_types5['MESSAGE'][0], values5]})
    # General Validations
    # erpsd_text = pd.read_sql(
    #     "SELECT RECORD_NO,TYPE,TEXT FROM O_RECORD_TEXT WHERE TYPE = 'SAPSFD' AND TEXT IS NOT NULL AND RECORD_NO = '{}' ".format(
    #         record_no), con)

    erpsd_text = pd.read_excel(
        r"/root/Desktop/Team Python Deployment/Gopi Projects/DH_RPA_Validations/DHS_RPA_Validations/o_record_text1.xls")
    erpsd_text = erpsd_text.loc[(erpsd_text['RECORD_NO'] == record_no) & (erpsd_text['TYPE'] == 'SAPSFD')]
    general_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
        DAL_RPA_TYPES WHERE ITEM = 'SAPSFD missing' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
        AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    general_type_val1 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM ='Reasearch URL missing' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    general_type_val2 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Incomplete PURCHSE des' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)
    general_type_val3 = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Cataloguing levels' AND FROM_STATUS = 'A1-REGISTERED (AA)' 
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    bu_level = pd.read_sql(
        "SELECT RECORD_NO,MODIFY_FLAG FROM O_RECORD_DHS_BU_LEVEL WHERE RECORD_NO = '{}' ".format(record_no), con)
    research_url = pd.read_sql(
        "SELECT * FROM ZZ_TEMP_OPS_RESEARCH WHERE MAT_ITM_NO = '{}' AND RESEARCH IS NOT NULL".format(record_no), con)
    document_data = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_NO,DOCUMENT_TYPE,REVISION,DOCUMENT_ITEM,ITEM_POSITION
                                FROM O_RECORD_DOCUMENT WHERE RECORD_NO = '{}' """.format(record_no), con)
    property_value = pd.read_sql("""SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1,PROPERTY_UOM FROM O_RECORD_CHAR 
                            WHERE PROPERTY_VALUE1 IS NOT NULL AND RECORD_NO = '{}' """.format(record_no), con)
    class_data = pd.read_sql("""SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' 
                AND CLASS_TERM IS NOT NULL and CLASS_TERM !='NO OBJECT'""".format(record_no), con)
    vendor_data = pd.read_sql(
        "SELECT RECORD_NO,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE RECORD_NO = '{}' ".format(record_no), con)
    refe_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_NO,REFERENCE_TYPE FROM O_RECORD_REFERENCE WHERE
                             RECORD_NO='{}' AND REFERENCE_NO != 'UNKNOWN' """.format(record_no), con)
    purchase_text = pd.read_sql("""SELECT RECORD_NO,TYPE,TEXT FROM O_RECORD_TEXT WHERE TYPE = 'PURCHASE' AND 
                TEXT IS NOT NULL AND RECORD_NO = '{}' """.format(record_no), con)
    document_data.fillna('', inplace=True)
    con.close()

    property_value['PROPERTY_UOM'] = property_value['PROPERTY_UOM'].apply(
        lambda x: '' if x in set(['N/A', 'NOT APPLICABLE']) else x)
    property_value['PROPERTY_VALUE2'] = property_value.apply(lambda x: x.PROPERTY_VALUE1 + ' ' + x.PROPERTY_UOM, axis=1)
    property_value['PROPERTY_VALUE2'] = property_value['PROPERTY_VALUE2'].apply(lambda x: x.strip())
    property_values_split = [re.split(r'[:,]', i) if re.search(',', i) else [i] for i in
                             property_value['PROPERTY_VALUE2']]
    property_values_split = list(itertools.chain(*property_values_split))
    property_values_split = [i.strip() for i in property_values_split if i]
    short_forms = pd.read_excel(r"/root/Desktop/Team Python Deployment/Gopi Projects/DH_RPA_Validations/DHS_RPA_Validations/V10_ORIG.xls")
    all_values = [list(document_data['DOCUMENT_NO']), list(property_value['PROPERTY_NAME']),
                  list(refe_data['REFERENCE_NO']),
                  list(vendor_data['VENDOR_NAME']), list(document_data['REVISION']),
                  list(document_data['DOCUMENT_ITEM']), list(document_data['ITEM_POSITION']),
                  list(property_value['PROPERTY_UOM']),
                  list(re.split(r'[\s,]', class_data['CLASS_TERM'][0])), property_values_split,
                  list(property_value['PROPERTY_VALUE1'])]
    all_values1 = list(itertools.chain(*all_values))
    all_values1 = [i.strip() for i in all_values1 if i]
    # all_values1 = list(itertools.chain(*all_values1))
    # print(all_values1)
    # all_values1 = [i.split(',') for i in all_values1 if re.search(',',i)]
    # all_values1 = list(itertools.chain(*all_values1))
    # print(all_values1)

    long_text = re.split(r'[,;:]', fft_text['MASTER_COLUMN6'][0])
    all_values1.extend(long_text)
    all_values1 = [i.strip() for i in all_values1 if i]
    print('all values', set(all_values1))

    purchase_values = re.split(r'[,;:]', re.sub(r'[\(\)]', '', purchase_text['TEXT'][0]))
    purchase_values = [i.strip() for i in purchase_values if i]
    print('purchase values', set(purchase_values))
    remaining_values = set(purchase_values).difference(set(all_values1))
    remaining_text = re.sub(r'[\(\)]', '', fft_text['MASTER_COLUMN10'][0])
    remove_short = [remaining_values.remove(i) for i in short_forms['ORIGINAL_PART_PREFIX'] if i in remaining_values]
    remove_short1 = [remaining_values.remove(i) for i in short_forms['V10_PART_PREFIX'] if i in remaining_values]

    # print(remove_short)
    print('remaining values', remaining_values)
    check_values = [i for i in remaining_values if not re.search(i, str(remaining_text))]
    check_values1 = ','.join(check_values)
    if len(check_values) >= 1:
        result_dict.update({message_data['SHORT_MESSAGE'][0]: [message_data['MESSAGE'][0], check_values1]})

    # result_dict = {}
    if erpsd_text.shape[0] == 0:
        result_dict.update({general_type_val['SHORT_MESSAGE'][0]: [general_type_val['MESSAGE'][0]]})
    if ('ENHANCED' in set(bu_level['MODIFY_FLAG'])) and (research_url.shape[0] == 0):
        result_dict.update({general_type_val1['SHORT_MESSAGE'][0]: [general_type_val1['MESSAGE'][0]]})
    # all_values = [list(document_data['DOCUMENT_NO']), list(property_value['PROPERTY_VALUE1']),
    #               list(refe_data['REFERENCE_NO']), list(re.split(r'[\s,]', class_data['CLASS_TERM'][0])),
    #               list(vendor_data['VENDOR_NAME']), list(document_data['REVISION']),
    #               list(document_data['DOCUMENT_ITEM']), list(document_data['ITEM_POSITION'])]
    # all_values = list(itertools.chain(*all_values))

    # print(all_values)
    # print(purchase_text['TEXT'][0])
    document_data = document_data.replace({'REVISION': {'0': ''}})
    check_all_values = []
    # check_all_values = [i for i in all_values if not re.search(i.strip(), purchase_text['TEXT'][0])]
    check_doc_values = [j + ' ' + str((i,)) for i, j in
                        zip(document_data['DOCUMENT_NO'], document_data['DOCUMENT_TYPE'])
                        if not re.search(i.strip(), purchase_text['TEXT'][0])]
    check_all_values.extend(check_doc_values)
    check_ref_values = [j + ' ' + str((i,)) for i, j in zip(refe_data['REFERENCE_NO'], refe_data['REFERENCE_TYPE'])
                        if not re.search(i.strip(), purchase_text['TEXT'][0])]
    check_all_values.extend(check_ref_values)
    check_prop_values = [j + ' ' + str((i,)) for i, j in zip(property_value['PROPERTY_VALUE1'],
                                                             property_value['PROPERTY_NAME']) if
                         not re.search(i.strip(), purchase_text['TEXT'][0])]
    check_all_values.extend(check_prop_values)
    check_vendor_values = [i for i in vendor_data['VENDOR_NAME']
                           if (not re.search(i.strip(), purchase_text['TEXT'][0])) and (i != 'UNKNOWN')]
    if len(check_vendor_values) > 0:
        check_all_values.append('VENDOR_NAME' + ' ' + str(tuple(check_vendor_values)))
    check_rev_values = [i for i in document_data['REVISION']
                        if (not re.search(i.strip(), purchase_text['TEXT'][0])) and (i != 'UNKNOWN')]
    if len(check_rev_values) > 0:
        check_all_values.append('REVISION' + ' ' + str(tuple(check_rev_values)))
    check_item_values = [i for i in document_data['DOCUMENT_ITEM']
                         if (not re.search(i.strip(), purchase_text['TEXT'][0])) and (i != 'UNKNOWN')]
    print('DOCUMENT_ITEM:', document_data['DOCUMENT_ITEM'])
    if len(check_item_values) > 0:
        check_all_values.append('DOCUMENT_ITEM' + ' ' + str(tuple(check_item_values)))
    check_pos_values = [i for i in document_data['ITEM_POSITION']
                        if (not re.search(i.strip(), purchase_text['TEXT'][0])) and (i != 'UNKNOWN')]
    if len(check_pos_values) > 0:
        check_all_values.append('ITEM_POSITION' + ' ' + str(tuple(check_pos_values)))
    valueToBeRemoved = 'UNKNOWN'
    try:
        while True:
            check_all_values.remove(valueToBeRemoved)
    except ValueError:
        pass
    check_all_values1 = ','.join(check_all_values)
    if len(check_all_values) >= 1:
        result_dict.update(
            {general_type_val2['SHORT_MESSAGE'][0]: [general_type_val2['MESSAGE'][0], check_all_values1]})
    print(set(bu_level['MODIFY_FLAG']), class_data.shape[0])
    if ('ENHANCED' in set(bu_level['MODIFY_FLAG'])) and (research_url.shape[0] == 0):
        result_dict.update({general_type_val3['SHORT_MESSAGE'][0]: [general_type_val3['MESSAGE'][0]]})
    elif ('ADVANCED' in set(bu_level['MODIFY_FLAG'])) and (property_value.shape[0] == 0):
        result_dict.update({general_type_val3['SHORT_MESSAGE'][0]: [general_type_val3['MESSAGE'][0]]})
    elif ('BASIC' in set(bu_level['MODIFY_FLAG'])) and (class_data.shape[0] == 0):
        result_dict.update({general_type_val3['SHORT_MESSAGE'][0]: [general_type_val3['MESSAGE'][0]]})
    return result_dict
