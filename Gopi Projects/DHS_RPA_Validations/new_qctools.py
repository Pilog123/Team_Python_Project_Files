from pickle import FALSE
import pandas as pd
import cx_Oracle
import re
import nltk
import urlfetch
import warnings
from similar_pattern import generate_pattern
from Vendor_Name_IN_FFT import VENDOR_FFT_VALIDATION
from Char_Values_IN_FFT import CHAR_FFT_VALIDATION
from Class_Terms_IN_FFT import CLASS_FFT_VALIDATION
from DocumentNo_IN_FFT import DOCUMENT_NO_FFT_VALIDATION
from ReferenceNo_IN_FFT import REFERENCE_NO_FFT_VALIDATION
from Same_Long_Desc_Diff_Class import LONG_DES_CLASS_VALIDATION
from Same_Short_Desc_Diff_Class import SHORT_DESC_CLASS_VALIDATION
from spellchecker import SpellChecker
from word_replacement import po_percentage
from nltk.corpus import stopwords
from nltk.corpus import words
from tqdm import tqdm

warnings.filterwarnings('ignore')
nltk.download('words')
word_list = words.words()


def same_value(data, data1, column, column1):
    indx_2 = []
    for parts in data1[column1].unique():
        parts_check = data.get_group(parts)
        if parts_check.shape[0] > 1:
            parts_check.drop_duplicates(subset=[column], keep='first', inplace=True)
        else:
            pass
        if parts_check.shape[0] > 1:
            indx_2.extend(list(parts_check.index))
    return indx_2


def similar_data(data, column, column1):
    copy_data = data
    values = pd.DataFrame()
    print('start')
    regex_patterns = [generate_pattern(i) for i in copy_data[column]]
    print('done')
    for i in regex_patterns:
        matched_values = [j for j in copy_data[column] if re.search(r'^{}$'.format(i), j)]
        if len(matched_values) >= 1:
            reference_data1 = copy_data[copy_data[column].isin(matched_values)]
            reference_data1.drop_duplicates(subset=[column1], keep='first', inplace=True)
            if reference_data1.shape[0] > 1:
                values = pd.concat([values, reference_data1])
    return values


def unwanted_spaces(data, column):
    regex_pattern = {'VALUE': r"[:,\-_+*/.<>]{2,}|\s{2,}|[^:,\-_+*/.< >\(\)\w]",
                     'LONG_DESC': r'[:;,\-_+*/.<>&]{2,}|\s{2,}|[^:;,\-_+*/.< >&\(\)\w]'}
    value_check = [True if not re.search(r"{}".format(regex_pattern[column]), i) else False for i
                   in data[column]]
    data['UNWANTED_SPACES'] = value_check
    return data


def qc_checks(orgn_id, batch_id):
    # con_str = cx_Oracle.connect("DR1024216/Pipl#mdrm$216@172.16.1.13:1522/DR101411")
    con_str = cx_Oracle.connect(
        "DR1024218/PIPL_mdrm#218@155.248.248.202:1521/PDBSHYAM.shyamdalmia.shyamdalmiadb.oraclevcn.com")
    reference_data = pd.read_sql("""SELECT RECORD_NO,REFERENCE_TYPE,REFERENCE_NO,VENDOR_NAME FROM 
        O_RECORD_REFERENCE WHERE REFERENCE_NO != 'UNKNOWN' AND RECORD_NO IN (SELECT RECORD_NO FROM O_RECORD_REGISTER 
        WHERE ORGN_ID='{}' AND REGISTER_COLUMN5 = '{}')""".format(orgn_id, batch_id), con_str)

    document_data = pd.read_sql("""SELECT RECORD_NO,DOCUMENT_TYPE,DOCUMENT_NO,DOC_VENDOR_NAME FROM O_RECORD_DOCUMENT 
                                WHERE RECORD_NO IN (SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE ORGN_ID='{}' AND 
                                REGISTER_COLUMN5 = '{}') AND DOCUMENT_NO != 'UNKNOWN' """.format(orgn_id, batch_id),
                                con_str)

    property_data = pd.read_sql("""SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1,PROPERTY_UOM FROM O_RECORD_CHAR WHERE 
                                PROPERTY_VALUE1 IS NOT NULL AND RECORD_NO IN (SELECT RECORD_NO FROM O_RECORD_REGISTER 
                                WHERE ORGN_ID='{}' AND REGISTER_COLUMN5 = '{}')
                                AND PROPERTY_VALUE1 != 'UNKNOWN' """.format(orgn_id, batch_id), con_str)

    class_data = pd.read_sql("""SELECT RECORD_NO, CLASS_TERM,ABBREVIATION, MASTER_COLUMN5, MASTER_COLUMN6,MASTER_COLUMN10 FROM O_RECORD_MASTER WHERE 
                                RECORD_NO IN(SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE ORGN_ID='{}' 
                                AND REGISTER_COLUMN5 = '{}')""".format(orgn_id, batch_id), con_str)

    fft_text = pd.read_sql("""SELECT RECORD_NO,CLASS_TERM,MASTER_COLUMN6, MASTER_COLUMN10 FROM O_RECORD_MASTER WHERE 
                            RECORD_NO IN(SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE ORGN_ID='{}' 
                            AND REGISTER_COLUMN5 = '{}')""".format(orgn_id, batch_id), con_str)

    org_terminology = pd.read_sql("""SELECT TERM FROM ORGN_TERMINOLOGY WHERE CONCEPT_TYPE = 'Class' 
                AND LANGUAGE = 'English US'""", con_str)
    purchase_text = pd.read_sql("""SELECT RECORD_NO, LOCALE, TYPE, TEXT FROM O_RECORD_TEXT WHERE RECORD_NO IN(SELECT RECORD_NO FROM O_RECORD_REGISTER 
                        WHERE ORGN_ID='{}' AND REGISTER_COLUMN5 = '{}') AND TYPE = 'PURCHASE' AND LOCALE='en_US' """.format(
        orgn_id, batch_id), con_str)
    # property_words = pd.read_sql("SELECT VALUE,ABBREVIATION FROM DAL_RPA_PROP_10", con_str)
    # uom_values = pd.read_sql("SELECT CLASS,RULE_UOM,PROPERTY FROM DAL_RPA_PROP_6 ", con_str)
    # print('start')
    property_words = pd.read_excel("./str_abbreviation.xls")
    uom_values = pd.read_excel("./str_uom.xls")
    property_uom = pd.read_sql("""SELECT RECORD_NO,PROPERTY_NAME,PROPERTY_VALUE1, PROPERTY_UOM FROM O_RECORD_CHAR WHERE 
    PROPERTY_UOM NOT IN('N/A','NOT APPLICABLE') AND RECORD_NO IN(SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE 
    ORGN_ID='{}' AND REGISTER_COLUMN5 = '{}') """.format(orgn_id, batch_id), con_str)
    bu_level = pd.read_sql("""SELECT RECORD_NO,MODIFY_FLAG FROM O_RECORD_DHS_BU_LEVEL WHERE RECORD_NO IN (SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE 
    ORGN_ID='{}' AND REGISTER_COLUMN5 = '{}') AND MODIFY_FLAG = 'ENHANCED' """.format(orgn_id, batch_id), con_str)
    research_url = pd.read_sql("""SELECT RECORD_NO, TEXT FROM O_RECORD_DH_RESEARCH WHERE RECORD_NO IN
    (SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE ORGN_ID='{}' AND 
    REGISTER_COLUMN5 = '{}') """.format(orgn_id, batch_id), con_str)
    property_uom1 = pd.read_sql("""SELECT RECORD_NO,REQUIRED_FLAG FROM 
    O_RECORD_CHAR WHERE REQUIRED_FLAG = 'Y' AND RECORD_NO IN(SELECT RECORD_NO FROM O_RECORD_REGISTER WHERE 
    ORGN_ID='{}' AND REGISTER_COLUMN5 = '{}')""".format(orgn_id, batch_id), con_str)
    # class_data1 = pd.read_sql("""SELECT * FROM DAL_RPA_CLASS_5 """, con_str)
    class_data1 = pd.read_excel("./std_keywords.xls")
    # prop_4 = pd.read_sql("""SELECT * FROM DAL_RPA_CLASS_4 """, con_str)
    prop_4 = pd.read_excel("./std_dimensions.xls")
    # print('end')
    con_str.close()
    replace_values = {'N/A': '', 'NOT APPLICABLE': ''}
    property_data = property_data.replace({'PROPERTY_UOM': replace_values})
    print(property_data)
    # print(property_data['PROPERTY_UOM'])
    property_data['PROPERTY_VALUE1'] = property_data.apply(lambda uom: uom.PROPERTY_VALUE1 + ' ' + uom.PROPERTY_UOM, axis=1)
    property_data.drop(['PROPERTY_UOM'], axis=1, inplace=True)
    property_data['VENDOR_NAME'] = ''
    reference_data.columns = ['RECORD_NO', 'NAME', 'VALUE', 'VENDOR_NAME']
    document_data.columns = ['RECORD_NO', 'NAME', 'VALUE', 'VENDOR_NAME']
    property_data.columns = ['RECORD_NO', 'NAME', 'VALUE', 'VENDOR_NAME']
    class_data.columns = ['RECORD_NO', 'CLASS', 'ABBREVIATION', 'SHORT_DESC', 'LONG_DESC', 'FFT']
    reference_data['TABLE_NAME'] = 'O_RECORD_REFERENCE'
    document_data['TABLE_NAME'] = 'O_RECORD_DOCUMENT'
    property_data['TABLE_NAME'] = 'O_RECORD_CHAR'
    resp_data = {}
    total_data = pd.concat([reference_data, document_data, property_data], ignore_index=True)
    final_data = total_data.merge(class_data, on='RECORD_NO')
    final_data = final_data.loc[~(final_data['VALUE'].isin(['UNKOWN', 'UNNKOWN']))]
    check_data = final_data[final_data.duplicated(['RECORD_NO', 'VALUE'], keep=False)]
    final_data = final_data[
        ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME', 'CLASS', 'ABBREVIATION', 'SHORT_DESC', 'LONG_DESC', 'FFT',
         'VENDOR_NAME']]
    #     # final_data.to_excel('total_data{}.xlsx'.format(batch_id),index=False)
    display_data = pd.DataFrame()
    check_data = final_data[final_data.duplicated(['RECORD_NO', 'VALUE'], keep=False)]
    if (check_data is not None) and (len(check_data) > 1):
        display_data = pd.concat([display_data, check_data], ignore_index=True)
    # display_data.to_excel("same_value_within_record{}.xlsx".format(batch_id), index=False)
    # display_data = display_data.loc[:, ['RECORD_NO', 'NAME','CLASS','ABBREVIATION','VALUE',
    # 'TABLE_NAME','SHORT_DESC','LONG_DESC','FFT','VENDOR_NAME']]
    display_data['VALIDATION_NAME'] = 'SAME VALUE WITHIN RECORD'
    resp_data.update({'same_value_within_record': display_data.to_json(orient='records')})
    class_group = final_data.groupby(by=['CLASS'])
    diff_abbr = pd.DataFrame()
    for i in final_data['CLASS'].unique():
        class_abbreviation_data = class_group.get_group(i)
        # diff_abbr = class_abbreviation_data[class_abbreviation_data.duplicated(['CLASS'], keep=False)]
        class_abbreviation_data = class_abbreviation_data.drop_duplicates(subset=['ABBREVIATION'], keep='first',
                                                                          inplace=True)
        if (class_abbreviation_data is not None) and (class_abbreviation_data.shape[0] > 1):
            diff_abbr = pd.concat([diff_abbr, class_abbreviation_data], ignore_index=True)
    if not diff_abbr.empty:
        diff_abbr['VALIDATION_NAME'] = 'SAME CLASS DIFFERENT ABBREVIATION'
        resp_data.update({'same_value_within_record': diff_abbr.to_json(orient='records')})
    # diff_abbr.to_excel('same_class_diff_abbreviation.xlsx', index=False)
    ref_data = final_data.loc[final_data['TABLE_NAME'] == 'O_RECORD_REFERENCE']
    ref_doc_data = final_data.loc[final_data['TABLE_NAME'].isin(['O_RECORD_REFERENCE', 'O_RECORD_DOCUMENT'])]
    # doc_data = final_data.loc[final_data['TABLE_NAME'] == 'O_RECORD_DOCUMENT']
    # ref_data_groupby = ref_data.groupby(by=['VALUE'])
    doc_data_groupby = ref_doc_data.groupby(by=['VALUE'])
    # ref_indx = same_value(ref_data_groupby, ref_data, 'NAME', 'VALUE')
    doc_indx = same_value(doc_data_groupby, ref_doc_data, 'NAME', 'VALUE')
    ref_final_data = ref_doc_data.loc[ref_doc_data.index.isin(doc_indx)].sort_values(by=['VALUE'])
    # doc_final_data = doc_data.loc[doc_data.index.isin(doc_indx)].sort_values(by=['VALUE'])
    # result_data = pd.concat([ref_final_data, doc_final_data], ignore_index=True)
    # result_data = result_data.loc[:, ['RECORD_NO', 'NAME','CLASS','VALUE','ABBREVIATION',
    # 'TABLE_NAME','SHORT_DESC','LONG_DESC','FFT','VENDOR_NAME']]
    ref_final_data['VALIDATION_NAME'] = 'SAME VALUE DIFFERENT TYPE'
    # ref_final_data.to_excel("same_value_diff_type{}.xlsx".format(batch_id), index=False)
    resp_data.update({'same_value_diff_type': ref_final_data.to_json(orient='records')})

    final_data1 = final_data.copy()
    diff_format = pd.DataFrame()
    final_data1['striped_values'] = final_data1['VALUE'].apply(lambda x: re.sub(r'[\W]', '', x))
    final_data_values = final_data1.groupby(by=['VALUE'])
    for i in final_data1['VALUE'].unique():
        get_values_data = final_data_values.get_group(i)
        get_values_data.drop_duplicates(subset=['striped_values'], keep='first', inplace=True)
        if (get_values_data is not None) and (get_values_data.shape[0] > 1):
            diff_format = pd.concat([diff_format, get_values_data], ignore_index=True)
    diff_format.drop_duplicates(keep='first', inplace=True)
    resp_data.update({'same_value_diff_format': diff_format.to_json(orient='records')})

    reference_data1 = final_data.loc[final_data['TABLE_NAME'] == 'O_RECORD_REFERENCE']
    document_data1 = final_data.loc[final_data['TABLE_NAME'] == 'O_RECORD_DOCUMENT']
    similar_ref = similar_data(reference_data1, 'VALUE', 'NAME')
    similar_doc = similar_data(document_data1, 'VALUE', 'NAME')
    similar_result_data = pd.concat([similar_ref, similar_doc], ignore_index=True)
    # similar_result_data = similar_result_data.loc[:, ['RECORD_NO', 'NAME','CLASS','ABBREVIATION',
    # 'VALUE', 'TABLE_NAME','SHORT_DESC','LONG_DESC','FFT','VENDOR_NAME']]
    similar_result_data['VALIDATION_NAME'] = 'SIMILAR VALUE DIFFERENT TYPE'
    similar_result_data.drop_duplicates(keep='first', inplace=True)
    # similar_result_data.to_excel("similar_ref_doc_diff_types{}.xlsx".format(batch_id), index=False)
    resp_data.update({'similar_ref_doc_diff_types': similar_result_data.to_json(orient='records')})

    partno_data = final_data.loc[final_data['NAME'].isin(['OEM PART NO', 'MANUFACTURER PART NO', 'SUPPLIER PART NO'])]
    partno_data_groupby = partno_data.groupby(by=['VALUE'])
    partno_indx = same_value(partno_data_groupby, partno_data, 'CLASS', 'VALUE')
    part_final_data = partno_data.loc[
        partno_data.index.isin(partno_indx), ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME', 'CLASS', 'ABBREVIATION',
                                              'SHORT_DESC', 'LONG_DESC', 'FFT', 'VENDOR_NAME']].sort_values(
        by=['VALUE'])
    # part_final_data.to_excel("same_partno_diff_class{}.xlsx".format(batch_id), index=False)
    part_final_data['VALIDATION_NAME'] = 'SAME PARTNO DIFFERENT CLASS'
    part_final_data.drop_duplicates(keep='first', inplace=True)
    resp_data.update({'same_partno_diff_class': part_final_data.to_json(orient='records')})

    # print('vendor')
    vendor_data = ref_data.loc[ref_data['VENDOR_NAME'] != 'UNKNOWN']
    print(vendor_data)
    vendor_data.dropna(how='any', inplace=True, axis=0)
    vendor_data_groupby = vendor_data.groupby(by=['VENDOR_NAME'])
    vendor_indx = same_value(vendor_data_groupby, vendor_data, 'NAME', 'VENDOR_NAME')

    vendor_final_data = vendor_data.loc[vendor_data.index.isin(vendor_indx),
                                        ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME', 'CLASS', 'ABBREVIATION',
                                         'SHORT_DESC', 'LONG_DESC', 'FFT', 'VENDOR_NAME']].sort_values(
        by=['VENDOR_NAME'])
    # vendor_final_data.to_excel('same_vendor_diff_types{}.xlsx'.format(batch_id), index=False)
    vendor_final_data['VALIDATION_NAME'] = 'SAME VENDOR DIFFERENT TYPES'
    vendor_final_data.drop_duplicates(keep='first', inplace=True)
    resp_data.update({'same_vendor_diff_types': vendor_final_data.to_json(orient='records')})

    ref_vendor_group = vendor_data.groupby(by=['VALUE'])
    ref_vendor_index = same_value(ref_vendor_group, vendor_data, 'VENDOR_NAME', 'VALUE')
    ref_vendor_final_data = vendor_data.loc[vendor_data.index.isin(ref_vendor_index),
                                            ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME', 'CLASS', 'ABBREVIATION',
                                             'SHORT_DESC', 'LONG_DESC', 'FFT', 'VENDOR_NAME']].sort_values(
        by=['VALUE'])
    # ref_vendor_final_data.to_excel('same_value_diff_vendors{}.xlsx'.format(batch_id), index=False)
    ref_vendor_final_data['VALIDATION_NAME'] = 'SAME VALUE DIFFERENT VENDORS'
    ref_vendor_final_data.drop_duplicates(keep='first', inplace=True)
    resp_data.update({'same_value_diff_vendors': ref_vendor_final_data.to_json(orient='records')})

    similar_ref_vendor = similar_data(vendor_data, 'VALUE', 'VENDOR_NAME')
    similar_ref_vendor = similar_ref_vendor.loc[:,
                         ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME', 'CLASS', 'ABBREVIATION', 'SHORT_DESC',
                          'LONG_DESC', 'FFT', 'VENDOR_NAME']]
    similar_ref_vendor['VALIDATION_NAME'] = 'SIMILAR REFERENCE DIFFERENT VENDOR'
    similar_ref_vendor.drop_duplicates(keep='first', inplace=True)
    # similar_ref_vendor.to_excel("similar_ref_diff_vendor{}.xlsx".format(batch_id), index=False)
    resp_data.update({'similar_ref_diff_vendor': similar_ref_vendor.to_json(orient='records')})

    vendor_data1 = vendor_data.loc[:, ['RECORD_NO', 'NAME', 'VALUE', 'VENDOR_NAME', 'TABLE_NAME']]
    vendor_fft = VENDOR_FFT_VALIDATION(fft_text, vendor_data1)
    vendor_fft = vendor_fft.loc[:, ['RECORD_NO', 'MASTER_COLUMN10', 'VENDOR_NAME', 'VENDOR_NAME_IN_FFT']]
    vendor_fft1 = final_data.loc[(final_data['RECORD_NO'].isin(list(vendor_fft['RECORD_NO']))) &
                                 (final_data['VENDOR_NAME'].isin(list(vendor_fft['VENDOR_NAME'])))]
    # vendor_fft.columns = ['RECORD_NO', 'FFT', 'VENDOR_NAME', 'VENDOR_NAME_IN_FFT']
    vendor_fft1['VALIDATION_NAME'] = 'VENDOR NAME IN FFT'
    vendor_fft1.drop_duplicates(keep='first', inplace=True)
    # vendor_fft1.to_excel("vendor_name_in_fft{}.xlsx".format(batch_id), index=False)
    resp_data.update({'vendor_name_in_fft': vendor_fft1.to_json(orient='records')})

    char_data = final_data.loc[final_data['TABLE_NAME'] == 'O_RECORD_CHAR']
    char_fft = CHAR_FFT_VALIDATION(char_data)
    # char_fft = char_fft.loc[:, ['RECORD_NO', 'MASTER_COLUMN10', 'PROPERTY_VALUE1', 'CHAR_VALUES_IN_FFT']]
    # char_fft.columns = ['RECORD_NO', 'FFT', 'CHAR_VALUES', 'CHAR_VALUES_IN_FFT']
    print(char_fft)
    # char_fft1 = final_data.loc[(final_data['RECORD_NO'].isin(list(char_fft['RECORD_NO']))) &
    # (final_data['VALUE'].isin(list(char_fft['PROPERTY_VALUE1'])))]
    char_fft.drop(['CHAR_VALUES_IN_FFT'], axis=1, inplace=True)
    char_fft['VALIDATION_NAME'] = 'CHAR VALUES IN FFT'
    # char_fft.to_excel("char_in_fft{}.xlsx".format(batch_id), index=False)
    resp_data.update({'char_in_fft': char_fft.to_json(orient='records')})

    class_term = CLASS_FFT_VALIDATION(fft_text)
    class_term = class_term.loc[:, ['RECORD_NO', 'CLASS_TERM', 'MASTER_COLUMN10', 'CLASS_TERM_IN_FFT']]
    # class_term.columns = ['RECORD_NO', 'CLASS_TERM', 'FFT', 'CLASS_TERM_IN_FFT']
    class_term1 = final_data.loc[(final_data['RECORD_NO'].isin(list(class_term['RECORD_NO']))) &
                                 (final_data['CLASS'].isin(list(class_term['CLASS_TERM'])))]
    class_term1 = class_term1.loc[class_term1['TABLE_NAME'] == 'O_RECORD_REFERENCE']
    class_term1.drop_duplicates(subset=['RECORD_NO'], keep='first', inplace=True)
    class_term1['VALIDATION_NAME'] = 'CLASS IN FFT'
    # class_term1.to_excel("class_term_in_fft{}.xlsx".format(batch_id), index=False)
    resp_data.update({'class_term_in_fft': class_term1.to_json(orient='records')})

    ref_fft = REFERENCE_NO_FFT_VALIDATION(fft_text, reference_data)
    ref_fft = ref_fft.loc[:, ['RECORD_NO', 'MASTER_COLUMN10', 'REFERENCE_NO', 'REFERENCE_NO_IN_FFT']]
    # ref_fft.columns = ['RECORD_NO', 'FFT', 'REFERENCE_NO', 'REFERENCE_NO_IN_FFT']
    ref_fft1 = final_data.loc[(final_data['RECORD_NO'].isin(list(ref_fft['RECORD_NO']))) &
                              (final_data['TABLE_NAME'] == 'O_RECORD_REFERENCE')]
    ref_fft1['VALIDATION_NAME'] = 'REFERENCE IN FFT'
    ref_fft1.drop_duplicates(keep='first', inplace=True)
    # ref_fft1.to_excel("reference_in_fft{}.xlsx".format(batch_id), index=False)
    resp_data.update({'reference_in_fft': ref_fft1.to_json(orient='records')})

    doc_fft = DOCUMENT_NO_FFT_VALIDATION(fft_text, document_data)
    doc_fft = doc_fft.loc[:, ['RECORD_NO', 'MASTER_COLUMN10', 'DOCUMENT_NO', 'DOCUMENT_NO_IN_FFT']]
    # doc_fft.columns = ['RECORD_NO', 'FFT', 'DOCUMENT_NO', 'DOCUMENT_NO_IN_FFT']
    doc_fft1 = final_data.loc[(final_data['RECORD_NO'].isin(list(doc_fft['RECORD_NO']))) &
                              (final_data['TABLE_NAME'] == 'O_RECORD_DOCUMENT')]
    doc_fft1['VALIDATION_NAME'] = 'DOCUMENT IN FFT'
    doc_fft1.drop_duplicates(keep='first', inplace=True)
    # doc_fft1.to_excel("document_in_fft{}.xlsx".format(batch_id), index=False)
    resp_data.update({'document_in_fft': doc_fft1.to_json(orient='records')})

    long_desc = LONG_DES_CLASS_VALIDATION(fft_text)
    # long_desc['BATCH_ID'] = batch_id
    # long_desc.columns = ['RECORD_NO','CLASS_TERM','LONG_DESC']
    long_desc1 = final_data.loc[final_data['RECORD_NO'].isin(list(long_desc['RECORD_NO']))]
    long_desc1['VALIDATION_NAME'] = 'SAME LONG DESC DIFFERENT CLASS'
    long_desc.drop_duplicates(subset=['RECORD_NO'], keep='first', inplace=True)
    # long_desc1.to_excel('same_long_desc_diff_class{}.xlsx'.format(batch_id), index=False)
    resp_data.update({'same_long_desc_diff_class': long_desc1.to_json(orient='records')})

    short_desc = SHORT_DESC_CLASS_VALIDATION(final_data)
    short_desc['VALIDATION_NAME'] = 'SAME SHORT DESC DIFFERENT CLASS'
    short_desc.drop_duplicates(subset=['RECORD_NO'], keep='first', inplace=True)
    # short_desc.to_excel('same_short_desc_diff_class{}.xlsx'.format(batch_id), index=False)
    resp_data.update({'same_short_desc_diff_class': short_desc.to_json(orient='records')})

    # orgn_index = []
    # for i in range(fft_text.shape[0]):
    #     orgn_terminology = org_terminology.loc[org_terminology['TERM'] == fft_text['CLASS_TERM'][i]]
    #     if orgn_terminology.shape[0] == 0:
    #         # result_dict.update({char_type_val2['SHORT_MESSAGE'][0]: [char_type_val2['MESSAGE'][0]]})
    #         orgn_index.append(i)
    # result_data2 = fft_text.loc[fft_text.index.isin(orgn_index)]
    # result_data2 = result_data2.loc[:, ['RECORD_NO', 'CLASS_TERM']]
    # # result_data2['BATCH_ID'] = batch_id
    # result_data3 = final_data.loc[final_data['RECORD_NO'].isin(list(result_data2['RECORD_NO']))]
    #     # result_data3.to_excel('Incorrect_class{}.xlsx'.format(batch_id), index=False)
    # resp_data.update({'Incorrect_class': result_data3.to_json(orient='records')})

    property_data.columns = ['RECORD_NO', 'NAME', 'VALUE', 'VENDOR_NAME', 'TABLE_NAME']

    miss_val = []
    spell = SpellChecker()
    spell.word_frequency.load_words(list(property_words['ABBREVIATION']))
    spell.word_frequency.load_words(word_list)
    for k in property_data['VALUE']:
        if (len(k) > 2) and (not re.search(r'[^a-zA-z\s]', k)):
            values = k.split()
            misspelled = spell.unknown(values)
            if len(misspelled) >= 1:
                miss_val.append(','.join(misspelled))
            else:
                miss_val.append(' ')
        else:
            miss_val.append(' ')
    miss_spelled_words = property_data.copy()
    miss_spelled_words['MISSPELLED_WORDS'] = miss_val
    miss_spelled_words = miss_spelled_words.loc[miss_spelled_words['MISSPELLED_WORDS'] != ' ',
                                                ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME',
                                                 'MISSPELLED_WORDS']]
    # miss_spelled_words['BATCH_ID'] = batch_id
    miss_spelled_words1 = final_data.loc[(final_data['RECORD_NO'].isin(list(miss_spelled_words['RECORD_NO']))) &
                                         (final_data['VALUE'].isin(list(miss_spelled_words['VALUE'])))]
    miss_spelled_words1['VALIDATION_NAME'] = 'MISS SPELLED WORDS'
    # miss_spelled_words1.to_excel("Miss_spelled_words{}.xlsx".format(batch_id), index=False)
    miss_spelled_words.drop_duplicates(keep='first', inplace=True)
    resp_data.update({'Miss_spelled_words': miss_spelled_words1.to_json(orient='records')})

    # miss_val1 = []
    # spell_1 = SpellChecker()
    # spell_1.word_frequency.load_words(list(property_words['ABBREVIATION']))
    # spell_1.word_frequency.load_words(list(property_words['VALUE']))
    # spell_1.word_frequency.load_words(word_list)
    # for k in property_data['VALUE']:
    #     if (len(k) > 2) and (not re.search(r'[^a-zA-z\s]', k)):
    #         values = k.split()
    #         misspelled1 = spell_1.unknown(values)
    #         if len(misspelled1) >= 1:
    #             miss_val1.append(','.join(misspelled1))
    #         else:
    #             miss_val1.append(' ')
    #     else:
    #         miss_val1.append(' ')
    # miss_spelled_words1 = property_data.copy()
    # miss_spelled_words1['NON_APPROVED'] = miss_val1
    # miss_spelled_words1 = miss_spelled_words1.loc[
    #     miss_spelled_words1['NON_APPROVED'] != ' ', ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME']]
    # miss_spelled_words1[
    #     'NON_APPROVED_ABBREVATION'] = """it might be either a spelling mistake (or)
    #     standard rules of abbreviation are not followed"""
    # # miss_spelled_words1['BATCH_ID'] = batch_id
    #     # miss_spelled_words1.to_excel("non_approved_abbreviation{}.xlsx".format(batch_id), index=False)
    # resp_data.update({'non_approved_abbreviation': miss_spelled_words1.to_json(orient='records')})

    property_uom = property_uom.merge(class_data, on='RECORD_NO')
    property_uom_group = property_uom.groupby(by='CLASS')
    final_uom_values = pd.DataFrame()
    for i in property_uom['CLASS'].unique():
        check_uom = property_uom_group.get_group(i)
        uom_values1 = uom_values.loc[uom_values['CLASS'] == i]
        check_uom = check_uom.loc[check_uom['PROPERTY_NAME'].isin(list(uom_values1['PROPERTY']))]
        check_uom_values = check_uom.loc[~(check_uom['PROPERTY_UOM'].isin(list(uom_values1['RULE_UOM'])))]
        if check_uom_values.shape[0] >= 1:
            final_uom_values = pd.concat([final_uom_values, check_uom_values], ignore_index=True)

    final_uom_values = final_uom_values.loc[:, ['RECORD_NO', 'PROPERTY_NAME', 'PROPERTY_VALUE1', 'PROPERTY_UOM']]
    final_uom_values['PROPERTY_VALUE1'] = final_uom_values.apply(
        lambda uom_value: uom_value.PROPERTY_VALUE1 + uom_value.PROPERTY_UOM, axis=1)
    # final_uom_values['BATCH_ID'] = batch_id
    final_uom_values1 = final_data.loc[(final_data['RECORD_NO'].isin(list(final_uom_values['RECORD_NO']))) &
                                       (final_data['VALUE'].isin(list(final_uom_values['PROPERTY_VALUE1'])))]
    final_uom_values['VALIDATION_NAME'] = 'INCORRECT UOM'
    final_uom_values.drop_duplicates(keep='first', inplace=True)
    # final_uom_values1.to_excel("Incorrect_uom{}.xlsx".format(batch_id), index=False)
    resp_data.update({'Incorrect_uom': final_uom_values1.to_json(orient='records')})

    unwanted_spaces_char = unwanted_spaces(final_data, 'VALUE')
    unwanted_spaces_char = unwanted_spaces_char.loc[
        unwanted_spaces_char['UNWANTED_SPACES'] == False]
    # unwanted_spaces_char['BATCH_ID'] = batch_id
    unwanted_spaces_char.drop(['UNWANTED_SPACES'], axis=1, inplace=True)
    unwanted_spaces_char['VALIDATION_NAME'] = 'UNWANTED SPACES IN VALUES'
    unwanted_spaces_char.drop_duplicates(keep='first', inplace=True)
    # unwanted_spaces_char.to_excel("unwanted_spaces_in_char{}.xlsx".format(batch_id), index=False)
    resp_data.update({'unwanted_spaces_in_char': unwanted_spaces_char.to_json(orient='records')})

    # unwanted_spaces_desc = unwanted_spaces(class_data, 'LONG_DESC')
    # unwanted_spaces_desc = unwanted_spaces_desc.loc[
    #     unwanted_spaces_desc['UNWANTED_SPACES'] == False, ['RECORD_NO', 'CLASS', 'LONG_DESC']]
    # # unwanted_spaces_desc['BATCH_ID'] = batch_id
    #     # unwanted_spaces_desc.to_excel("unwanted_spaces_in_desc{}.xlsx".format(batch_id), index=False)
    # resp_data.update({'unwanted_spaces_in_desc': unwanted_spaces_desc.to_json(orient='records')})
    final_data.drop(['UNWANTED_SPACES'], axis=True, inplace=True)
    final_property_value = final_data.loc[final_data['TABLE_NAME'] == 'O_RECORD_CHAR', :]
    print(final_property_value)
    property_value_group = final_property_value.groupby(by=['CLASS', 'VALUE'])
    repeted_values = pd.DataFrame()
    for cls, value in zip(final_property_value['CLASS'], final_property_value['VALUE']):
        property_value1 = property_value_group.get_group((cls, value))
        # print(property_value1)
        # values = [j for i, j in zip(property_value1['VALUE'], property_value1['RECORD_NO']) if
        #             list(property_data['VALUE']).count(i) >= 2]
        # values = property_value1[property_value1.duplicated(subset=['VALUE'], keep=False)]
        # print(values['VALUE'])
        property_value1.drop_duplicates(subset=['NAME'], keep='first', inplace=True)
        # values.sort_values(by=['VALUE'], inplace=True)
        # print(values)
        if len(property_value1) > 1:
            # prop_no.extend(list(set(values)))
            repeted_values = pd.concat([property_value1, repeted_values], ignore_index=True)
    # repeted_values = property_data.loc[property_data['RECORD_NO'].isin(prop_no),:]
    repeted_values.drop_duplicates(inplace=True)
    repeted_values = repeted_values.loc[:,
                     ['RECORD_NO', 'NAME', 'VALUE', 'TABLE_NAME', 'CLASS', 'ABBREVIATION', 'SHORT_DESC', 'LONG_DESC',
                      'FFT', 'VENDOR_NAME']]
    # repeted_values.to_excel("same_class_value_diff_type{}.xlsx".format(batch_id), index=False)
    repeted_values['VALIDATION_NAME'] = 'SAME CLASS VALUE DIFFERENT TYPE'
    repeted_values.drop_duplicates(keep='first', inplace=True)
    resp_data.update({'char_values_repeated': repeted_values.to_json(orient='records')})

    dime_indx = []
    for j in tqdm(range(class_data.shape[0])):
        txt = class_data['LONG_DESC'][j]
        cls = class_data['CLASS'][j]
        class_data2 = class_data1.loc[class_data1['CLASS'].str.contains(cls)]
        if class_data2.shape[0] >= 1:
            val_lst_2 = []
            for qu_key, in_des, in_cls in zip(class_data1['QUALIFIER_KEYWORD'], class_data1['CLASS_KEYWORD'],
                                              class_data1['CLASS']):
                # if (qu_key not in txt) and (in_des not in txt) and (in_cls != cls):
                #     val_lst_1.append(True)
                if (re.search(qu_key, txt)) and (re.search(in_des, cls)) and (in_cls != cls):
                    # print(in_cls,cls)
                    val_lst_2.append(True)
                    break
                else:
                    pass
            else:
                pass
            # print('values:', val_lst_1)
            if (len(val_lst_2) != 0) and (any(val_lst_2)):
                # result_dict.update({char_type_val1['SHORT_MESSAGE'][0]: [char_type_val1['MESSAGE'][0]]})
                # print(cls)
                dime_indx.append(j)
    class_dimension = class_data.loc[
        class_data.index.isin(dime_indx), ['RECORD_NO', 'CLASS', 'SHORT_DESC', 'LONG_DESC', 'FFT']]
    class_dimension1 = final_data.loc[final_data['RECORD_NO'].isin(list(class_dimension['RECORD_NO']))]
    class_dimension1['VALIDATION_NAME'] = 'PREDOMINANT MATERIAL BASED CLASS'
    class_dimension1.drop_duplicates(subset=['RECORD_NO'], keep='first', inplace=True)
    # class_dimension1.to_excel("PreDominant_Material_Based_Class{}.xlsx".format(batch_id), index=False)
    resp_data.update({'PreDominant_Material_Based_Class': class_dimension1.to_json(orient='records')})

    # bool_values = []
    # print(len(bool_values))
    # result_dict = {}
    # dimentions_based_class = pd.DataFrame()
    # property_uom1.dropna(axis=0,how='any',inplace=True)

    # property_uom1['PROPERTY_VALUE2'] = property_uom1['PROPERTY_VALUE1'].apply(lambda x: x if (not re.search(r'[A-Za-z\s]|^0',str(x))or(x.isnumeric())) else '')
    # property_uom1['PROPERTY_VALUE2'] = property_uom1['PROPERTY_VALUE2'].apply(lambda x:float(str(x))if x.isnumeric() else x)
    #     # # property_uom1.to_excel('check.xlsx', index=False)
    # property_uom1 = property_uom1.loc[property_uom1['PROPERTY_VALUE2'] !=' ',:]
    # property_uom1.dropna(axis=0,how='any',inplace=True)
    # property_uom1['PROPERTY_VALUE2'] = property_uom1.apply(lambda uom: float(eval(uom['PROPERTY_VALUE2'])) * 25.5
    # if (uom['PROPERTY_UOM'] == 'IN')and(isinstance(uom['PROPERTY_VALUE2'],float)) else uom['PROPERTY_VALUE2'], axis=1)

    # # property_uom1['PROPERTY_VALUE2'] = property_uom1.apply(lambda uom:values_change(uom.PROPERTY_VALUE2,uom.PROPERTY_UOM))
    # property_uom_group = property_uom1.groupby(by=['RECORD_NO'])
    # prop_4['CLASS'] = prop_4['CLASS'].apply(lambda cla: re.sub(r'\*', '', cla))
    # for record in property_uom1['RECORD_NO'].unique():
    #     class_data3 = class_data.loc[class_data['RECORD_NO']==record,:].reset_index(drop=True)
    #     class_presence = [i for i in prop_4['CLASS'] if i in class_data3['CLASS'][0]]
    #     if len(class_presence) > 0:
    #         # for i in char_data['PROPERTY_NAME']:
    #         propert_value = prop_4[prop_4['CLASS'].isin(class_presence)].reset_index(drop=True)
    #         char_data = property_uom_group.get_group(record)
    #         char_data1 = char_data.loc[char_data['PROPERTY_NAME'].isin(propert_value['PROPERTY'])].reset_index(drop=True)
    #         # print(char_data1)S
    #         # print(char_data1)
    #         if char_data1.shape[0] > 0:
    #             propert_value = propert_value[['VALUE', 'CLASS']]
    #             for k, j in zip(propert_value['VALUE'], propert_value['CLASS']):
    #                 # print(char_data1)
    #                 expre = ''.join(re.sub(r'[a-zA-Z\d]', '', str(k)))
    #                 number = '{}'.format(''.join(re.findall(r'\d', str(k))))
    #                 # print(number, expre)
    #                 final_check = eval(f"float(char_data1['PROPERTY_VALUE2'][0]){expre}{float(number)}")
    #                 # print(final_check)
    #                 if not final_check:
    #                     dimentions_based_class = pd.concat([dimentions_based_class,char_data1],ignore_index=True)
    #                     break
    #                 # print(expre)

    #         else:
    #             pass
    # # dimentions_based_class = property_uom.loc[property_uom['RECORD_NO'].isin(bool_values),:]
    # print(dimentions_based_class)
    # if not dimentions_based_class.empty:
    #     dimentions_based_class = dimentions_based_class.merge(class_data,how='left',on='RECORD_NO')
    #     dimentions_based_class = dimentions_based_class.loc[:,['RECORD_NO','PROPERTY_NAME','PROPERTY_VALUE1','PROPERTY_VALUE2','PROPERTY_UOM','CLASS']]
    #     # dimentions_based_class.to_excel("dimenstion_based_class{}.xlsx".format(batch_id),index=False)
    # resp_data.update({"dimenstion_based_class":dimentions_based_class.to_json(orient='records')})

    # research_indx = []
    # for i, j in enumerate(property_uom1['RECORD_NO']):
    #     missing_research_url = research_url.loc[research_url['RECORD_NO'] == j]
    #     if missing_research_url.shape[0] == 0:
    #         research_indx.append(i)
    #     elif missing_research_url.shape[0] >= 1:
    #         if not re.search(r'(HTTP|http|www|.com)', missing_research_url['TEXT'][i]):
    #             research_indx.append(i)
    # missing_urls = bu_level.loc[bu_level.index.isin(research_indx), :]
    # missing_urls['VALIDATION_NAME'] = 'research_urls_missing'
    # # missing_urls.to_excel("research_urls_missing{}.xlsx".format(batch_id), index=False)
    # resp_data.update({'research_urls_missing': missing_urls.to_json(orient='records')})
    #
    # research_records = []
    #
    # for record, url in zip(research_url['RECORD_NO'], research_url['TEXT']):
    #     url1 = "{}".format(url)
    #     try:
    #         response = urlfetch.get(url1, timeout=20, max_redirects=10)
    #     except Exception as e:
    #         research_records.append(record)
    # urls_not_valid = research_url.loc[research_url['RECORD_NO'].isin(research_records)]
    # urls_not_valid['VALIDATION_NAME'] = 'URLS NOT VALID'
    # # urls_not_valid.to_excel('urls_not_valid{}.xlsx'.format(batch_id), index=False)
    # resp_data.update({"urls_not_valid": urls_not_valid.to_json(orient='records')})
    #
    # words_exclude = po_percentage(purchase_text, fft_text)
    # words_exclude.columns = ['RECORD_NO', 'LOCALE', 'TEXT', 'LONG_DESC', 'UNMATCHED_WORDS']
    # words_exclude['VALIDATION_NAME'] = 'WORDS EXCLUDE'
    # resp_data.update({"words_exclude": words_exclude.to_json(orient='records')})
    # words_exclude.to_excel('words_exclude{}.xlsx'.format(batch_id), index=False)
    # print(words_exclude)
    return resp_data
# qc_checks('992F97AD777A4E40925986E94C151707','TAPCO_PILOT')
