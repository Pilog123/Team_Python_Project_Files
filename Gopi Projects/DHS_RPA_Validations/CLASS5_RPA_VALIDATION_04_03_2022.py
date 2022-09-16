import pandas as pd
import cx_Oracle


def validation_rpa_dimentions(recordno):
    con = cx_Oracle.connect("DR1024216/Pipl#mdrm$216@172.16.1.13:1522/DR101411")
    
    input_data = pd.read_sql('''SELECT RECORD_NO,CLASS_TERM,MASTER_COLUMN6 FROM
                             O_RECORD_MASTER WHERE RECORD_NO='{}' '''.format(recordno), con)

    class_data = pd.read_sql("""SELECT * FROM DAL_RPA_CLASS_5 WHERE CLASS = '{}' """.format(input_data['CLASS_TERM'][0]),  con)

    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Class differs as per dimentions' AND FROM_STATUS = 'A1-REGISTERED (AA)'
            AND TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)                          
    con.close()
    # print(inputData)
    txt = input_data['MASTER_COLUMN6'][0]
    cls = input_data['CLASS_TERM'][0]
    val_lst_1 = []
    for qu_key, in_des, in_cls in zip(class_data['QUALIFIER_KEYWORD'], class_data['CLASS_KEYWORD'], class_data['CLASS']):
        if (qu_key not in txt) and (in_des not in txt) and (in_cls != cls):
            val_lst_1.append(True)
        elif (qu_key in txt) and (in_des in txt) and (in_cls == cls):
            val_lst_1.append(False)        
    if all(val_lst_1):
        result_dict = {char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0]]}
        return result_dict
    # else:
    #     result_dict = {char_type_val['SHORT_MESSAGE'][0]: ["Sucess"]}
    #     return result_dict
