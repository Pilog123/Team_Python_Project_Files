from pickle import NONE

from regex import F
from isk_connectit import DatabaseConnection
import CLASS_ALLOCATION_TEST, CLASS_ALLOCATION_TEST_28_03_2022, CLASS_ALLOCATION_19_04_2022
import REFERENCE
# import CHAR_ALLOCATION, CHAR_ALLOCATION_SRAVANTH_21
import CHAR_ALLOCATION_LIVE
import time
from datetime import datetime
# import CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD
import ONLY_CLASS_TEST
# import CLASS_ALLOCATION_LAYER
import uvicorn
from fastapi import FastAPI, Form, Depends
from typing import Optional
from pydantic import BaseModel
import numpy as np
import pandas as pd
import warnings
import sys
# import flask
from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
# warnings.simplefilter(action='ignore', category=FutureWarning)
# warnings.filterwarnings('ignore')


allocationApp = FastAPI(title="Class/UNSPSC Allocation",
                        description="""This project prepares proper Object/Class data
                         in qualifying the data relies on user level.""", version="1.0")





import json
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


def CallingDatabaseTable(accessName, qry):
    connection = DatabaseConnection(accessName)

    if connection.accessNameValidation() == 'YES':
        print(connection.getDBProperties())
        propertiesData = connection.getDBProperties()
        # conn_str = connection.MakeConnectionFormat('DR1024193', 'Pipl#mdrm$93', '172.16.1.61', '1521', 'DR101412')
        conn_str = connection.MakeConnectionFormat(propertiesData['USERNAME'], propertiesData['PASSWORD'], propertiesData['HOST'], propertiesData['PORT'], propertiesData['SERVICE'])
        print(conn_str, '\n')
        # qry = " select 'ISK_'||sys_guid() from dual "
        tableData = connection.GetConnected(conn_str, qry)
        print(tableData)
        return tableData
    else:
        print("""Mr. Data provider, please check the 'accessName' you have given!""")
        return """Mr. Data provider, please check the 'accessName' you have given!"""


def CallingDatabaseConnectionStr(accessName):
    connection = DatabaseConnection(accessName)

    if connection.accessNameValidation() == 'YES':
        print(connection.getDBProperties())
        propertiesData = connection.getDBProperties()
        # conn_str = connection.MakeConnectionFormat('DR1024193', 'Pipl#mdrm$93', '172.16.1.61', '1521', 'DR101412')
        conn_str = connection.MakeConnectionFormat(propertiesData['USERNAME'], propertiesData['PASSWORD'], propertiesData['HOST'], propertiesData['PORT'], propertiesData['SERVICE'])
        print(conn_str, '\n')
        # qry = " select 'ISK_'||sys_guid() from dual "
        #tableData = connection.GetConnected(conn_str, qry)
        print(conn_str)
        return conn_str
    else:
        print("""Mr. Data provider, please check the 'accessName' you have given!""")
        return """Mr. Data provider, please check the 'accessName' you have given!"""



class reqDataFormat(BaseModel):
    tableName: str
    colsArray: str
    accessName: str
    BATCH_ID: str
    accessName: str



# async def dataQualifying(tableName: Optional[str], colsArray: Optional[str], accessName: Optional[str], BATCH_ID: Optional[str], analysisType: Optional[str]):
@allocationApp.get("/classallocation/")
@allocationApp.post("/classallocation/")
async def dataQualifying(tableName: Optional[str] = Form(...), colsArray: Optional[str] = Form(...), accessName: Optional[str] = Form(...), SPIR_NO: Optional[str] = Form(None), analysisType: Optional[str] = Form(...), responseId: Optional[str] = Form(...), createBy: Optional[str] = Form(None), BATCH_ID: Optional[str] = Form(None)):

    # BATCH_ID = SPIR_NO
    return_response_data = None
    final_res = None
    print('-------------------------- ', analysisType, '-------------------------- ')
    import cx_Oracle

    if analysisType == 'CLASS_ALLOCATION':  # API RESPONSE TABLE UPDATE
        # start_time = time.time()
        start_time = datetime.now()
        print(colsArray, tableName, BATCH_ID)

        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        print(tuple(BATCH_ID.split(',')), BATCH_ID.split(','))
        # batch_split_str = "',".join(i for i in BATCH_ID.split(','))
        # batch_split_str = "'{}'".format(i for i in BATCH_ID.split(','))
        # print('--->', batch_split_str)
        #
        # batch_split_str = batch_split_str.strip(",").replace(" ", "")
        # print('FInal BATCH SPLIT: ', batch_split_str)
        # s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        b_split_str = "".join("'" + i + "'," for i in BATCH_ID.split(','))
        print('---->', b_split_str.strip(","))
        b_split_str = b_split_str.strip(",")
        # s_qry = """ select {} from {} where BATCH_ID in ({}) and create_by = '{}' """.format(colsArray, tableName, b_split_str, createBy)
        s_qry = """ select {} from {} where BATCH_ID in ({}) """.format(colsArray, tableName, b_split_str)

        print(s_qry)
        data = CallingDatabaseTable(accessName, s_qry)
        print(type(data))
        insert_conn_str = CallingDatabaseConnectionStr(accessName)
        # classAllication_dataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.class_allocation_func(data[:5])
        classAllication_dataset = CLASS_ALLOCATION_TEST.class_allocation_func(data)

        # unspscDataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.unspsc_allocation_func(classAllication_dataset)
        # unspscDataset = CLASS_ALLOCATION_TEST.unspsc_allocation_func(classAllication_dataset, insert_conn_str)
        # unspscDataset.to_excel('Class_records.xlsx')

        # return data.to_dict(orient='records')
        # return_response_data = unspscDataset.to_dict('records')
        return_response_data = classAllication_dataset.to_json(orient='records')

        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        # res_conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')

        #res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        #res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@152.67.17.244/PDBAWL.sub09250858450.pilogmdm.oraclevcn.com')
        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@152.67.17.244:1521/PDBAWL.sub09250858450.pilogmdm.oraclevcn.com')

        res_conn = cx_Oracle.connect(insert_conn_str)
        res_cr = res_conn.cursor()
        print('res_cr: ', res_cr)
        # final_res = {'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)}
        # final_res = return_response_data
        # rf = open('r.txt', 'w')
        # rf.write(str(final_res))

        # i_qry = """ insert into TEMP_PYTHON_PANDAS(RESPONSE_ID, RES_DATA) values(:RESPONSE_ID, :RES_DATA )"""
        i_qry = """ insert into PYTHON_API_RESPONSE(RESPONSE_ID, RESPONSE_CATEGORY, RESPONSE_DATA, INSERT_DATE) values(:RESPONSE_ID, :RESPONSE_CATEGORY, :RESPONSE_DATA, :INSERT_DATE)"""

        # res_cr.setinputsizes(RES_DATA = cx_Oracle.CLOB)
        # res_cr.execute(i_qry, RESPONSE_ID="3", RES_DATA=str(final_res))
        print(i_qry)
        # res_cr.execute(i_qry)

        res_cr.execute(i_qry, RESPONSE_ID=responseId, RESPONSE_CATEGORY = 'Class Allocation', RESPONSE_DATA = str(return_response_data), INSERT_DATE = str(datetime.now()))
        res_conn.commit()

        res_cr.close()
        res_conn.close()
        print('Response Inserted!')

        # CallingDatabaseTable(accessName, i_qry)
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))

    elif analysisType == 'CLASS_ALLOCATION_DB_UPDATE':  #CLASS, FFT UPDATE IN UNIFICATION_STG
        # start_time = time.time()
        start_time = datetime.now()
        print(colsArray, tableName, BATCH_ID)

        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        print(tuple(BATCH_ID.split(',')), BATCH_ID.split(','))
        # batch_split_str = "',".join(i for i in BATCH_ID.split(','))
        # batch_split_str = "'{}'".format(i for i in BATCH_ID.split(','))
        # print('--->', batch_split_str)
        #
        # batch_split_str = batch_split_str.strip(",").replace(" ", "")
        # print('FInal BATCH SPLIT: ', batch_split_str)
        # s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        b_split_str = "".join("'" + i + "'," for i in BATCH_ID.split(','))
        print('---->', b_split_str.strip(","))
        b_split_str = b_split_str.strip(",")
        # s_qry = """ select {} from {} where BATCH_ID in ({}) and create_by = '{}' """.format(colsArray, tableName, b_split_str, createBy)
        s_qry = """ select {} from {} where BATCH_ID in ({}) """.format(colsArray, tableName, b_split_str)

        print(s_qry)
        data = CallingDatabaseTable(accessName, s_qry)
        print(type(data))
        insert_conn_str = CallingDatabaseConnectionStr(accessName)
        # classAllication_dataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.class_allocation_func(data[:5])
        classAllication_dataset = CLASS_ALLOCATION_TEST.class_allocation_func(data)

        # unspscDataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.unspsc_allocation_func(classAllication_dataset)
        # unspscDataset = CLASS_ALLOCATION_TEST.unspsc_allocation_func(classAllication_dataset, insert_conn_str)
        # unspscDataset.to_excel('Class_records.xlsx')

        # return data.to_dict(orient='records')
        # return_response_data = unspscDataset.to_dict('records')
        return_response_data = classAllication_dataset.to_json(orient='records')

        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        # res_conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')

        #res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        #res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@152.67.17.244/PDBAWL.sub09250858450.pilogmdm.oraclevcn.com')
        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@152.67.17.244:1521/PDBAWL.sub09250858450.pilogmdm.oraclevcn.com')

        res_conn = cx_Oracle.connect(insert_conn_str)
        res_cr = res_conn.cursor()
        print('res_cr: ', res_cr)
        # final_res = {'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)}
        # final_res = return_response_data
        # rf = open('r.txt', 'w')
        # rf.write(str(final_res))

        # i_qry = """ insert into TEMP_PYTHON_PANDAS(RESPONSE_ID, RES_DATA) values(:RESPONSE_ID, :RES_DATA )"""
        # i_qry = """ insert into O_RECORD_DATA_UNIFICATION_STG(RECORD_NO, CLASS, INSERT_DATE) values(:RECORD_NO, :CLASS, :INSERT_DATE)"""
        uqy = """UPDATE O_RECORD_DATA_UNIFICATION_STG SET CLASS = :CLASS WHERE RECORD_NO = :RECORD_NO
                """
        print(uqy)
        for rec, cls in zip(classAllication_dataset['RECORD_NO'], classAllication_dataset['NEW_CLASS']):
            res_cr.execute(uqy, RECORD_NO=rec, CLASS = cls)
            res_conn.commit()
        
        
        # res_cr.setinputsizes(RES_DATA = cx_Oracle.CLOB)
        # res_cr.execute(i_qry, RESPONSE_ID="3", RES_DATA=str(final_res))
        # print(i_qry)
        # res_cr.execute(i_qry)


        res_cr1 = res_conn.cursor()
        print('res_cr1: ', res_cr1)
        # final_res = {'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)}
        # final_res = return_response_data
        # rf = open('r.txt', 'w')
        # rf.write(str(final_res))

        # i_qry = """ insert into TEMP_PYTHON_PANDAS(RESPONSE_ID, RES_DATA) values(:RESPONSE_ID, :RES_DATA )"""
        i_qry1 = """ insert into PYTHON_API_RESPONSE(RESPONSE_ID, RESPONSE_CATEGORY, RESPONSE_DATA, INSERT_DATE) values(:RESPONSE_ID, :RESPONSE_CATEGORY, :RESPONSE_DATA, :INSERT_DATE)"""

        # res_cr.setinputsizes(RES_DATA = cx_Oracle.CLOB)
        # res_cr.execute(i_qry, RESPONSE_ID="3", RES_DATA=str(final_res))
        print(i_qry1)
        # res_cr.execute(i_qry)
#str({"status":"Updated", "count":len(data)})
        # res_data = dict({"status":"Updated", "count":len(data)})
        res_cr1.execute(i_qry1, RESPONSE_ID=responseId, RESPONSE_CATEGORY = 'Class Allocation', RESPONSE_DATA = "Updated, {}".format(len(data)) , INSERT_DATE = str(datetime.now()))
        res_conn.commit()

        res_cr.close()
        res_cr1.close()
        res_conn.close()

        print('Response Inserted!')


        # CallingDatabaseTable(accessName, i_qry)
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
    elif analysisType == 'DUMMY_CLASS_ALLOCATION':
        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        print(s_qry)
        data = CallingDatabaseTable(accessName, s_qry)
        print(type(data))
        # classAllication_dataset = ONLY_CLASS_TEST.class_allocation_func(data)
        classAllication_dataset = CLASS_ALLOCATION_TEST.class_allocation_func(data[:0])
        print('classAllication_dataset---> ', classAllication_dataset)
        return_response_data = classAllication_dataset.to_dict('records')
    elif analysisType == 'REFERENCE_DATA_EXTRACTION':
        # start_time = time.time()
        start_time = datetime.now()
        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        # s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        b_split_str = "".join("'" + i + "'," for i in BATCH_ID.split(','))
        print('---->', b_split_str.strip(","))
        b_split_str = b_split_str.strip(",")
        # s_qry = """ select {} from {} where BATCH_ID in ({}) and create_by = '{}' """.format(colsArray, tableName, b_split_str, createBy)
        s_qry = """ select {} from {} where BATCH_ID in ({}) """.format(colsArray, tableName, b_split_str)

        print(s_qry)

        reference_data = CallingDatabaseTable(accessName, s_qry)

        insert_conn_str = CallingDatabaseConnectionStr(accessName)

        part_prefix_df = pd.read_excel('V10_ORIG.xls')

        isml_uom = pd.read_excel('uom_file.xlsx')

        print(type(reference_data))

        reference_data.to_excel("test123.xlsx")
        reference_dataset = REFERENCE.reference_data(reference_data, part_prefix_df, isml_uom)
        print('classAllication_dataset---> ', reference_dataset)
        reference_dataset.to_excel('reference_records.xlsx')
        return_response_data = reference_dataset.to_json(orient='records')

        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        # res_conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')
        # res_conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')
        #res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@152.67.17.244/PDBAWL.sub09250858450.pilogmdm.oraclevcn.com')

        res_conn = cx_Oracle.connect(insert_conn_str)
        res_cr = res_conn.cursor()
        # final_res = {'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)}
        # final_res = return_response_data
        # rf = open('r.txt', 'w')
        # rf.write(str(final_res))

        # i_qry = """ insert into TEMP_PYTHON_PANDAS(RESPONSE_ID, RES_DATA) values(:RESPONSE_ID, :RES_DATA )"""
        i_qry = """ insert into PYTHON_API_RESPONSE(RESPONSE_ID, RESPONSE_CATEGORY, RESPONSE_DATA, INSERT_DATE) values(:RESPONSE_ID, :RESPONSE_CATEGORY, :RESPONSE_DATA, :INSERT_DATE)"""

        # res_cr.setinputsizes(RES_DATA = cx_Oracle.CLOB)
        # res_cr.execute(i_qry, RESPONSE_ID="3", RES_DATA=str(final_res))
        print(i_qry)
        # res_cr.execute(i_qry)

        res_cr.execute(i_qry, RESPONSE_ID=responseId, RESPONSE_CATEGORY='Reference Data Extraction',
                       RESPONSE_DATA=str(return_response_data), INSERT_DATE=str(datetime.now()))
        res_conn.commit()

        res_cr.close()
        res_conn.close()
        print('Response Inserted!')

        # CallingDatabaseTable(accessName, i_qry)
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
        #------------------------------------------------------------------------------------

    elif analysisType == 'CHAR_ALLOCATION':
        # start_time = time.time()
        start_time = datetime.now()
        # clause_df = pd.read_excel("IS_CLAUSES.xls").iloc[:, :5]
        # prop_cndtn_df = pd.read_excel("Is_properties_condition_working.xls").iloc[:, :9]
        # # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        # s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        # print(s_qry)
        # char_data = CallingDatabaseTable(accessName, s_qry)
        # char_data = char_data[:500]
        # print(type(char_data))
        # file = pd.ExcelFile('ISML UOM_1.xls')
        # UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
        # UOM1 = pd.read_excel(file, 'uom')
        # charallocation_dataset = CHAR_ALLOCATION.char_allocation(clause_df, prop_cndtn_df, char_data[:100], UOM, UOM1)

        # clause_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\CHAR_EXCELS\Clauses-15.03.22.xlsx", 'Sheet2').iloc[:, :5]
        clause_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\Clauses-15.03.22.xlsx", 'Sheet2').iloc[:, :5]

        # prop_cndtn_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\CHAR_EXCELS\Is_properties_condition_working.xls", "Sheet4").iloc[:, :9]
        prop_cndtn_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\Property_Rules.xlsx")

        # sheet5_df = pd.read_excel("Is_properties_condition_working.xls", "Sheet5")
        # desc_cls_file = pd.read_excel(r'C:\Users\Administrator\PythonDeployment\Class Allocation\Server Files\CHAR_EXCELS\desc_replace.xlsx')
        desc_cls_file = pd.read_excel(r'C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\desc_replace.xlsx')
        uom_replace_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\uomv1.xlsx")


        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        # s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        # ===============================================================================================
        # b_split_str = "".join("'"+i+"'," for i in SPIR_NO.split(','))
        # print('---->', b_split_str.strip(","))
        # print(str(tuple(SPIR_NO.split(','))).strip(','))
        # b_split_str = "".join("'" + i + "'," for i in SPIR_NO.split(','))
        # print('---->', b_split_str.strip(","))
        # b_split_str = b_split_str.strip(",")
        # #================================================================================================
        # # s_qry = """ select {} from {} where BATCH_ID in ({}) and create_by = '{}' """.format(colsArray, tableName, b_split_str, createBy)
        # # s_qry = """ select {} from {} where BATCH_ID in ('{}') and  """.format(colsArray, tableName, BATCH_ID)
        # s_qry = """ select {} from {} where SPIR_NO in ({}) """.format(colsArray, tableName, b_split_str)

        s_qry = ""
        if BATCH_ID:
            b_split_str = "".join("'" + i + "'," for i in BATCH_ID.split(','))
            print('---->', b_split_str.strip(","))
            b_split_str = b_split_str.strip(",")
            s_qry = """ select {} from {} where BATCH_ID in ({}) """.format(colsArray, tableName, b_split_str)
        elif SPIR_NO:
            b_split_str = "".join("'" + i + "'," for i in SPIR_NO.split(','))
            print('---->', b_split_str.strip(","))
            b_split_str = b_split_str.strip(",")
            s_qry = """ select {} from {} where SPIR_NO in ({}) """.format(colsArray, tableName, b_split_str)


        # s_qry = """ select {} from {} """.format(colsArray, tableName)

        print('Query: ', s_qry)
        char_data = CallingDatabaseTable(accessName, s_qry)

        insert_conn_str = CallingDatabaseConnectionStr(accessName)
        # char_data = pd.read_excel('spir_res(25-03).xlsx').fillna('')
        print('shape of data', char_data.shape)
        # char_data = char_data[:500]
        print(type(char_data))
        print(char_data)
        # file = pd.ExcelFile('ISML UOM_1.xls')
        # UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
        # UOM1 = pd.read_excel(file, 'uom')


        # charallocation_dataset = CHAR_ALLOCATION_LIVE_Upto_14_07_2022.char_allocation(clause_df, prop_cndtn_df, char_data,
        #                                                                      desc_cls_file)

        # charallocation_dataset = CHAR_ALLOCATION_LIVE.char_allocation(clause_df, prop_cndtn_df, char_data,
        #                                                                      desc_cls_file)
        charallocation_dataset = CHAR_ALLOCATION_LIVE.char_allocation(clause_df, prop_cndtn_df, char_data, 
                                                                        desc_cls_file,uom_replace_df)


        print('charAllication_dataset---> ', charallocation_dataset)

        # charallocation_dataset.to_excel('Char_records.xlsx')
        return_response_data = charallocation_dataset.to_json(orient='records')

        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        # res_conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')
        #res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@172.16.1.53:1521/DR071407')
        # res_conn = cx_Oracle.connect('DR1024222/Pipl#mdrm$222@152.67.17.244:1521/PDBAWL.sub09250858450.pilogmdm.oraclevcn.com')

        res_conn = cx_Oracle.connect(insert_conn_str)
        res_cr = res_conn.cursor()
        # final_res = {'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)}
        # final_res = return_response_data
        # rf = open('r.txt', 'w')
        # rf.write(str(final_res))

        # i_qry = """ insert into TEMP_PYTHON_PANDAS(RESPONSE_ID, RES_DATA) values(:RESPONSE_ID, :RES_DATA )"""
        i_qry = """ insert into PYTHON_API_RESPONSE(RESPONSE_ID, RESPONSE_CATEGORY, RESPONSE_DATA, INSERT_DATE) values(:RESPONSE_ID, :RESPONSE_CATEGORY, :RESPONSE_DATA, :INSERT_DATE)"""

        # res_cr.setinputsizes(RES_DATA = cx_Oracle.CLOB)
        # res_cr.execute(i_qry, RESPONSE_ID="3", RES_DATA=str(final_res))
        print(i_qry)
        # res_cr.execute(i_qry)

        # =============================================================================
        res_cr.execute(i_qry, RESPONSE_ID=responseId, RESPONSE_CATEGORY='Char Extraction',
                       RESPONSE_DATA=str(return_response_data), INSERT_DATE=str(datetime.now()))
        res_conn.commit()
        # =============================================================================


        res_cr.close()
        res_conn.close()
        print('Response Inserted!')

        # CallingDatabaseTable(accessName, i_qry)
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
        # ------------------------------------------------------------------------------------
    else:
        return_response_data = """Mr. Data provider, Please check the "analysisType" you have given!"""
    # print('-'*25, 'FINAL RESPONSE', '-'*25, '\n', return_response_data)
    # res_dct = {}
    # res_dct.update({'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)})
    # return JSONResponse(
    #         status_code=200,
    #         content=jsonable_encoder({"detail": res_dct}),
    #     )
    rd = open('r_data.txt', 'w')
    rd.write(return_response_data)
    rd.close()
    return 'Done!'


if __name__ == '__main__':
    # uvicorn.run("allocation_caller_TEST:allocationApp", host='172.16.1.60', port=6653)
    uvicorn.run("allocation_caller_TEST:allocationApp", host='172.16.1.60', port=6653)



    # uvicorn.run("allocation_caller_TEST:allocationApp", host='172.16.1.60', port=6653, reload=True)
