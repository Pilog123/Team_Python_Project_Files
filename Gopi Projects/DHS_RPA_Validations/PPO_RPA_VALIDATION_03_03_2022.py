import pandas as pd
import cx_Oracle


def validation_rpa_ppo(record_no):
    con = cx_Oracle.connect("DR1024216/Pipl#mdrm$216@172.16.1.13:1522/DR101411")
    
    input_data = pd.read_sql('''SELECT RECORD_NO,CLASS_TERM FROM
                             O_RECORD_MASTER WHERE RECORD_NO='{}' '''.format(record_no), con)

    char_type_val = pd.read_sql('''SELECT DISTINCT(ITEM),MESSAGE,RECORD_TYPE,SEQUENCE,SHORT_MESSAGE,IMPACT FROM 
            DAL_RPA_TYPES WHERE ITEM = 'Class does not exist in PPO' AND FROM_STATUS = 'A1-REGISTERED (AA)' AND 
            TO_STATUS = 'A1-QC SUBMITTED (BA)' ''', con)

    orgn_terminology = pd.read_sql("""SELECT TERM FROM ORGN_TERMINOLOGY WHERE CONCEPT_TYPE = 'Class' 
                    AND LANGUAGE = 'English US' AND TERM = '{}'  """.format(input_data['CLASS_TERM'][0]), con)
    con.close()

    if orgn_terminology.shape[0] == 0:
        result_dict = {char_type_val['SHORT_MESSAGE'][0]: [char_type_val['MESSAGE'][0]]}
        return result_dict        
    # else:
    #     result_dict = {char_type_val['SHORT_MESSAGE'][0]: ["Sucess"]}
    #     return result_dict



