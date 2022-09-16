import pandas as pd
import re
import cx_Oracle
import string
import warnings
warnings.filterwarnings('ignore')
# from tqdm import tqdm


def sabic_validations(term, plant, from_status, to_status):
    conn = cx_Oracle.connect('DR1024106/DR1024106@172.16.1.98:1521/DR101409')
    validation_messages = pd.DataFrame()
    values = []
    domain = {'M': 'PRODUCT', 'S': 'SERVICE', 'E': 'EQUIPMENT'}
    domain_value = domain[term[0]]
    values.extend([term, plant, from_status, to_status])
    status = pd.read_sql(f"SELECT RECORD_NO,STATUS FROM O_RECORD_BU_LEVEL WHERE RECORD_NO = '{term}'", conn)
    lables = pd.read_sql("SELECT * FROM DAL_RPA_LABELS1", conn)
    lables_1 = lables.to_json(orient='records')
    # Material type validation
    df_4 = pd.read_sql(
        '''SELECT RECORD_NO,CLASS_TERM,RECORD_TYPE FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' '''.format(term), conn)
    material_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Validate Material Type' 
                                                                        AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    values.append(df_4['RECORD_TYPE'][0])
    values.append(df_4['CLASS_TERM'][0])
    if df_4.empty:
        validation_messages = pd.concat([material_type_val, validation_messages], ignore_index=True)
    else:
        df_5 = pd.read_sql(
            '''SELECT MAT_TYPE FROM ONTG_CLASSIFICATION WHERE TERM = '{}' '''.format(df_4['CLASS_TERM'][0]), conn)

        if (df_5.empty == True) or (df_4['RECORD_TYPE'][0] not in df_5['MAT_TYPE'][0]):
            validation_messages = pd.concat([material_type_val, validation_messages], ignore_index=True)

        else:
            material_type_val['Status'] = 'Success'
            validation_messages = pd.concat([material_type_val, validation_messages], ignore_index=True)

    # Attachments_validation
    df_1 = pd.read_sql('''SELECT RECORD_NO FROM O_RECORD_ATTACH WHERE RECORD_NO='{}' '''.format(term), conn)

    attachments_validation = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Validate Attachments' 
                                                                                AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    if df_1.shape[0] == 0:
        validation_messages = pd.concat([attachments_validation, validation_messages], ignore_index=True)
        values.append('N')
    else:
        attachments_validation['Status'] = 'Success'
        validation_messages = pd.concat([attachments_validation, validation_messages], ignore_index=True)
        values.append('Y')

    # Reference_validation
    df_2 = pd.read_sql('''SELECT RECORD_NO,REFERENCE_NO FROM O_RECORD_REFERENCE WHERE RECORD_NO = '{}' '''.format(term),
                       conn)
    reference_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Reference Data Availability' 
                                                                                    AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    refe = df_2[df_2['REFERENCE_NO'] == 'UNKNOWN']
    if refe.shape[0] != 0 or df_2.shape[0] == 0:
        validation_messages = pd.concat([reference_val, validation_messages], ignore_index=True)
        values.append('N')
    else:
        reference_val['Status'] = 'Success'
        validation_messages = pd.concat([reference_val, validation_messages], ignore_index=True)
        values.append('Y')

    # REPAIR_POLICY
    repair_pol = pd.read_sql(
        '''SELECT RECORD_NO,CLASS_TERM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' '''.format(term), conn)
    repair_policy_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Show the user to select correct Repair policy based on the class' 
                                                                                            AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    if repair_pol.shape[0] >= 1:
        repairble_table = pd.read_sql(
            "SELECT * FROM DAL_RPA_REPAIR_POLICY WHERE CLASS = '{}'".format(repair_pol['CLASS_TERM'][0]), conn)
        record_mint = pd.read_sql(
            "SELECT MATNR,REPAIR_POLICY FROM O_RECORD_MAINT_CLASSIFICATION WHERE MATNR = '{}'".format(term), conn)
        if (repairble_table.empty) | (record_mint.empty):
            validation_messages = pd.concat([repair_policy_val, validation_messages], ignore_index=True)
            values.append('Y')
        elif (repairble_table['REPAIR_POLICY'][0] == 'NOT REPAIRABLE') & (record_mint['REPAIR_POLICY'][0] == 'Y'):
            validation_messages = pd.concat([repair_policy_val, validation_messages], ignore_index=True)
            values.append('Y')
        elif (repairble_table['REPAIR_POLICY'][0] == 'REPAIRABLE') & (record_mint['REPAIR_POLICY'][0] == 'N'):
            validation_messages = pd.concat([repair_policy_val, validation_messages], ignore_index=True)
            values.append('Y')
        else:
            repair_policy_val['Status'] = 'Success'
            validation_messages = pd.concat([repair_policy_val, validation_messages], ignore_index=True)
            values.append('N')
    else:
        validation_messages = pd.concat([repair_policy_val, validation_messages], ignore_index=True)
        values.append('Y')

    # Finance/Stocking Policy
    stocking = pd.read_sql(
        "SELECT MATNR,WERKS,VERPR FROM O_RECORD_ACCOUNTING WHERE MATNR = '{}' AND WERKS = '{}' ".format(term, plant),
        conn)
    def_curr = pd.read_sql(f"SELECT PLANT,DEFAULT_CURRENCY FROM B_PLANT WHERE PLANT = '{plant}'", conn)
    def_curr.fillna('', axis=0, inplace=True)
    conv = pd.read_sql(
        f"SELECT FROM_CURRENCY,CONVERTION_RATE FROM B_CURRENCY_CONVERTION WHERE FROM_CURRENCY = '{def_curr['DEFAULT_CURRENCY'][0]}'",
        conn)
    valclas = pd.read_sql(
        f"SELECT MATNR,EQUIPMENT_CLASS FROM O_RECORD_MAINT_CLASSIFICATION WHERE MATNR = '{term}' AND WERKS = '{plant}'",
        conn)
    currency_type_val = pd.read_sql(
        "SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Finance/Stocking Policy' AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' ".format(
            domain_value), conn)

    stocking['VERPR'].fillna(0, inplace=True)
    conv['CONVERTION_RATE'].fillna(0, inplace=True)
    if any([stocking.empty, def_curr.empty, valclas.empty]):
        validation_messages = pd.concat([currency_type_val, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        cost = float(stocking['VERPR'][0]) * float(conv['CONVERTION_RATE'][0])
        if (valclas['EQUIPMENT_CLASS'][0] == 'I' and cost > float(50000)) | (
                valclas['EQUIPMENT_CLASS'][0] == 'C' and cost < float(50000)):
            validation_messages = pd.concat([currency_type_val, validation_messages], ignore_index=True)
            values.append('Y')
        else:
            currency_type_val['Status'] = 'Success'
            validation_messages = pd.concat([currency_type_val, validation_messages], ignore_index=True)
            values.append('N')

    # vendor_name validation
    vendor_name = pd.read_sql(
        '''SELECT RECORD_NO,VENDOR_NAME FROM O_RECORD_REFERENCE WHERE RECORD_NO = '{}' '''.format(term), conn)
    vendor_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Unknown Vendor name validation'
                                                                                     AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    vendor_unk = vendor_name[vendor_name['VENDOR_NAME'] == 'UNKNOWN']
    if vendor_unk.shape[0] != 0 or vendor_name.shape[0] == 0:
        validation_messages = pd.concat([vendor_val, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        vendor_val['Status'] = 'Success'
        validation_messages = pd.concat([vendor_val, validation_messages], ignore_index=True)
        values.append('N')

    # Url content in text validation
    lst = ['http://', ' https://', 'http', 'https', 'www.', '.com', 'www', 'com']
    url_text = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Pop up regarding Attachments' 
                                                                                        AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    df_3 = pd.read_sql(
        '''SELECT RECORD_NO,TEXT,TYPE FROM O_RECORD_TEXT WHERE RECORD_NO = '{}' AND TYPE='FFT' '''.format(term), conn)
    if df_3.empty == True or any(ele in df_3['TEXT'][0] for ele in lst) == False:
        validation_messages = pd.concat([url_text, validation_messages], ignore_index=True)
        values.append('Y')

    else:
        vendor_val['Status'] = 'Success'
        validation_messages = pd.concat([url_text, validation_messages], ignore_index=True)
        values.append('N')

    # too many characters in Additional short
    text_len = pd.read_sql(
        '''SELECT RECORD_NO,TYPE,TEXT FROM O_RECORD_TEXT WHERE RECORD_NO = '{}' AND TYPE = 'SFDFFT' '''.format(term),
        conn)
    pop_text = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Pop up regarding too many characters in Additional short' 
                                                                                            AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    if text_len.empty or len(text_len['TEXT'][0]) > 40:
        validation_messages = pd.concat([pop_text, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        pop_text['Status'] = 'Success'
        validation_messages = pd.concat([pop_text, validation_messages], ignore_index=True)
        values.append('N')

    # Validate Base UOM
    uom_data = pd.read_sql(
        '''SELECT RECORD_NO,CLASS_TERM,UOM FROM O_RECORD_MASTER WHERE RECORD_NO = '{}' '''.format(term), conn)
    uom_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Validate Base UOM' 
                                                                                        AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    if uom_data.empty == True:
        validation_messages = pd.concat([pop_text, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        df_4 = pd.read_sql('''SELECT UOM FROM DAL_RPA_BASE_UOM WHERE CLASS = '{}' '''.format(uom_data['CLASS_TERM'][0]),
                           conn)
        if df_4.loc[df_4['UOM'].isin(list(uom_data['UOM']))].shape[0] == 0:
            validation_messages = pd.concat([uom_type_val, validation_messages], ignore_index=True)
            values.append('Y')
        else:
            uom_type_val['Status'] = 'Success'
            validation_messages = pd.concat([uom_type_val, validation_messages], ignore_index=True)
            values.append('N')

    # Validate Characteristic values
    char_value = pd.read_sql(
        '''SELECT RECORD_NO,PROPERTY_VALUE1,REQUIRED_FLAG FROM O_RECORD_CHAR WHERE RECORD_NO='{}' '''.format(term),
        conn)
    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Validate Characteristic values' 
                                                                                        AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    if char_value.loc[(char_value['PROPERTY_VALUE1'].isnull() == False) & (char_value['REQUIRED_FLAG'] == 'Y')].shape[
        0] == 0:
        validation_messages = pd.concat([char_type_val, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        char_type_val['Status'] = 'Success'
        validation_messages = pd.concat([char_type_val, validation_messages], ignore_index=True)
        values.append('N')

    # Validate SPD,POD Check boxes
    ref_types = ['MODEL/MACHINE NO', 'SERIAL NO', 'TAG/EQUIP NO', 'FOR TAG/EQUIP NO', 'EQUIP/TAG MODEL NO',
                 'EQUIP/TAG SERIAL NO', 'MODEL/SERIAL/TAG NO', 'SUPPLIER PART NO']
    spdpod_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Validate SFD,POD Check boxes' 
                                                                                        AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    messages = {'N,N': spdpod_val['MESSAGE'][0],
                'N,Y': 'Please uncheck the Supplier (or) Equipment details from POD under Reference data tab.',
                'Y,N': 'Please uncheck the Supplier (or) Equipment details from SFD under Reference data tab.'}
    sfd_pod = pd.read_sql(
        '''SELECT RECORD_NO,REFERENCE_TYPE,R_STXT_FLAG,R_LTXT_FLAG FROM O_RECORD_REFERENCE WHERE RECORD_NO ='{}' '''.format(
            term), conn)
    sfd_pod = sfd_pod.loc[(sfd_pod['REFERENCE_TYPE'].isin(ref_types)) & (
                (sfd_pod['R_STXT_FLAG'] == 'N') | (sfd_pod['R_LTXT_FLAG'] == 'N'))].reset_index(drop=True)
    if sfd_pod.shape[0] == 1:
        spdpod_val['MESSAGE'] = messages['{},{}'.format(sfd_pod['R_STXT_FLAG'][0], sfd_pod['R_LTXT_FLAG'][0])]
        validation_messages = pd.concat([spdpod_val, validation_messages], ignore_index=True)
        values.append('Y')

    elif sfd_pod.shape[0] > 1:
        try:
            spdpod_val['MESSAGE'] = messages['{},{}'.format(sfd_pod['R_STXT_FLAG'][0], sfd_pod['R_LTXT_FLAG'][1])]
            validation_messages = pd.concat([spdpod_val, validation_messages], ignore_index=True)
            values.append('Y')
        except:
            spdpod_val['MESSAGE'] = messages['{},{}'.format(sfd_pod['R_STXT_FLAG'][1], sfd_pod['R_LTXT_FLAG'][0])]
            validation_messages = pd.concat([spdpod_val, validation_messages], ignore_index=True)
            values.append('Y')
    elif sfd_pod.shape[0] == 0:
        spdpod_val['MESSAGE'] = spdpod_val['MESSAGE'][0]
        validation_messages = pd.concat([spdpod_val, validation_messages], ignore_index=True)
        values.append('Y')

    else:
        spdpod_val['Status'] = 'Success'
        validation_messages = pd.concat([spdpod_val, validation_messages], ignore_index=True)
        values.append('N')

    # Validate shelf life
    shelf = pd.read_sql(
        '''SELECT MATNR,REQ_SHELF_LIFE_IND,MHDRZ, MHDHB FROM O_RECORD_BASIC_DATA WHERE MATNR ='{}' '''.format(term),
        conn)
    shelf_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Validate shelf life' AND RECORD_TYPE='ERSA' 
                                                                                                        AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    shelf.fillna(0, axis=0, inplace=True)
    mrp = pd.read_sql("SELECT MATNR,PLIFZ FROM O_RECORD_MRP WHERE MATNR = '{}'".format(term), conn)
    mrp.fillna(0, axis=0, inplace=True)
    if (shelf.empty) | (mrp.empty):
        validation_messages = pd.concat([shelf_type_val, validation_messages], ignore_index=True)
        values.append('Y')
    elif shelf['REQ_SHELF_LIFE_IND'][0] == 'Y' and int(shelf['MHDRZ'][0]) != int(shelf['MHDHB'][0]) - int(
            mrp['PLIFZ'][0]):
        validation_messages = pd.concat([shelf_type_val, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        shelf_type_val['Status'] = 'Success'
        validation_messages = pd.concat([shelf_type_val, validation_messages], ignore_index=True)
        values.append('N')

    # Target stock justification points
    comments = pd.read_sql('''SELECT MATNR,JUSTIFICATION_COMMENTS FROM O_RECORD_MRP WHERE MATNR = '{}' '''.format(term),
                           conn)
    justification_message_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Target stock justification points' 
                                                                                    AND RECORD_TYPE='ERSA' AND DOMAIN = '{}' '''.format(
        domain_value), conn)

    comments['JUSTIFICATION_COMMENTS'].fillna('', axis=0, inplace=True)
    if comments.empty:
        validation_messages = pd.concat([justification_message_val, validation_messages], ignore_index=True)
        values.append('Y')
    else:
        s = comments['JUSTIFICATION_COMMENTS'][0]
        if (s == '') | (len(s) < 20) | (all(i in string.punctuation for i in s)):
            validation_messages = pd.concat([justification_message_val, validation_messages], ignore_index=True)
            values.append('Y')
        elif s.isdigit() | s.count(s[0]) == len(s) | re.search('[\W]{3,10}', s):
            validation_messages = pd.concat([justification_message_val, validation_messages], ignore_index=True)
            values.append('Y')
        else:
            justification_message_val['Status'] = 'Success'
            validation_messages = pd.concat([justification_message_val, validation_messages], ignore_index=True)
            values.append('N')

    # Missing BOM
    bom = pd.read_sql(f"SELECT RECORD_NO,PLANT FROM O_RECORD_BOM WHERE RECORD_NO='{term}' AND PLANT='{plant}'", conn)
    bom_val_types = pd.read_sql(
        f"SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM DAL_RPA_TYPES WHERE ITEM = 'Missing BOM' AND RECORD_TYPE='ERSA' AND DOMAIN = '{domain_value}' ",
        conn)
    if bom.shape[0] == 0:
        validation_messages = pd.concat([bom_val_types, validation_messages], ignore_index=True)
        # values.append('Y')
    else:
        bom_val_types['Status'] = 'Success'
        validation_messages = pd.concat([bom_val_types, validation_messages], ignore_index=True)
        # values.append('N')

    data = [values]
    cursor = conn.cursor()
    # cursor.prepare("""INSERT INTO O_RECORD_RPA_HISTORY(RECORD_NO,BUSINESS_UNIT,SOURCE_STATUS,TARGET_STATUS,RECORD_TYPE,CLASS,ATTACHMENT,REFERENCE,
    # REPAIR_ISSUE,FINANCE_STOKING_ISSUE,VENDOR_NAME_ISSUE,REFERENCE_URL_ISSUE,ADDITIONAL_SHORT_TEXT_ISSUE,BASE_UOM_ISSUE,CHARACTERISTIC_VALUES_ISSUE,
    # SFD_POD_ISSUE,SHELF_LIFE_ISSUE,JUSTIFICATION_COMMENTS_ISSUE,NOT_LINKED_WITH_EQUIPMENT_ISSU
    # ) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18)""")
    cursor.prepare("""INSERT INTO O_RECORD_RPA_HISTORY(RECORD_NO,BUSINESS_UNIT,SOURCE_STATUS,TARGET_STATUS,RECORD_TYPE,CLASS,ATTACHMENT,REFERENCE,
    REPAIR_ISSUE,FINANCE_STOKING_ISSUE,VENDOR_NAME_ISSUE,REFERENCE_URL_ISSUE,ADDITIONAL_SHORT_TEXT_ISSUE,BASE_UOM_ISSUE,CHARACTERISTIC_VALUES_ISSUE,
    SFD_POD_ISSUE,SHELF_LIFE_ISSUE,JUSTIFICATION_COMMENTS_ISSUE
    ) VALUES (:0,:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17)""")
    cursor.executemany(None, data)
    conn.commit()
    cursor.close()
    conn.close()
    validation_messages.sort_values(by=['SEQUENCE'], inplace=True)
    validation_messages = validation_messages.loc[validation_messages['SHORT_MESSAGE'].isnull() == False]
    validation_messages['Status'].fillna('', axis=0, inplace=True)
    validation_messages['Status'] = validation_messages[['Status']].apply(lambda x: list(x), axis=1)

    validation_messages = validation_messages.loc[:, ['MESSAGE', 'SHORT_MESSAGE', 'IMPACT', 'Status']]
    validation_messages['MESSAGE_1'] = validation_messages[['MESSAGE', 'IMPACT']].apply(lambda x: list(x), axis=1)
    validation_messages.reset_index(drop=True, inplace=True)
    success_messages = {}
    for i in range(validation_messages.shape[0]):
        if validation_messages['Status'][i][0] == '':
            x = validation_messages.loc[i:i + 1, ['SHORT_MESSAGE', 'MESSAGE_1']]
            validation_messages_dct = dict(zip(x['SHORT_MESSAGE'], x['MESSAGE_1']))
            success_messages.update(validation_messages_dct)
        else:
            y = validation_messages.loc[i:i + 1, ['SHORT_MESSAGE', 'Status']]
            validation_messages_dct = dict(zip(y['SHORT_MESSAGE'], y['Status']))
            success_messages.update(validation_messages_dct)
    validation_messages_dct_1 = {'messages': success_messages, 'lables': lables_1}
    return validation_messages_dct_1
