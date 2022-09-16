from isk_connectit import DatabaseConnection
import CLASS_ALLOCATION_TEST
import REFERENCE
import CHAR_ALLOCATION, CHAR_ALLOCATION_LIVE
import Char_Final_for_SPIRdata_demo_09_03_22_with_exclude
import time
from datetime import datetime
# import CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD
import ONLY_CLASS_TEST
# import CLASS_ALLOCATION_LAYER
import uvicorn
from fastapi import FastAPI, Form, Depends, Response
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
warnings.filterwarnings('ignore')


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



class reqDataFormat(BaseModel):
    tableName: str
    colsArray: str
    accessName: str
    BATCH_ID: str
    accessName: str



# async def dataQualifying(tableName: Optional[str], colsArray: Optional[str], accessName: Optional[str], BATCH_ID: Optional[str], analysisType: Optional[str]):
@allocationApp.get("/classallocation")
@allocationApp.post("/classallocation")
# async def dataQualifying(tableName: Optional[str] = Form(...), colsArray: Optional[str] = Form(...), accessName: Optional[str] = Form(...), SPIR_NO: Optional[str] = Form(...), analysisType: Optional[str] = Form(...)):
async def dataQualifying(tableName: Optional[str] = Form(...), colsArray: Optional[str] = Form(...), accessName: Optional[str] = Form(...), BATCH_ID: Optional[str] = Form(None), SPIR_NO: Optional[str] = Form(None), analysisType: Optional[str] = Form(...)):


    return_response_data = None
    print('-------------------------- ', analysisType, '-------------------------- ')

    if analysisType == 'UNSPSC_CLS':
        # start_time = time.time()
        start_time = datetime.now()
        print(colsArray, tableName, BATCH_ID)

        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)

        # s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        # s_qry = """ select {} from {} where BATCH_ID in {} """.format(colsArray, tableName, tuple(BATCH_ID.split(',')),
        #                                                               BATCH_ID.split(','))
        b_split_str = "".join("'" + i + "'," for i in SPIR_NO.split(','))
        print('---->', b_split_str.strip(","))
        b_split_str = b_split_str.strip(",")
        s_qry = """ select {} from {} where BATCH_ID in ({}) """.format(colsArray, tableName, b_split_str)
        print(s_qry)
        data = CallingDatabaseTable(accessName, s_qry)
        print(type(data))
        # import cx_Oracle
        # cnn = cx_Oracle.connect('DR1024187/Pipl#mdrm$187@172.16.1.55:1521/CUSINPDB')
        # #--------------
        # cnn = cx_Oracle.connect(r'PPRM_REP_SSD/PPRM_REP_SSD@172.16.1.33:1521/PDBVISIONSSD')
        # d = pd.read_sql(s_qry, cnn)
        # print(d)
        # cnn.close()
        # #--------------

        # classAllication_dataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.class_allocation_func(data[:5])
        classAllication_dataset = CLASS_ALLOCATION_TEST.class_allocation_func(data[:50])

        # unspscDataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.unspsc_allocation_func(classAllication_dataset)
        unspscDataset = CLASS_ALLOCATION_TEST.unspsc_allocation_func(classAllication_dataset)
        unspscDataset.to_excel('Class_records.xlsx')

        # return data.to_dict(orient='records')
        return_response_data = unspscDataset.to_dict('records')
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
        # s_qry = """ select {} from {} where BATCH_ID in {} """.format(colsArray, tableName, tuple(BATCH_ID.split(',')),
        #                                                               BATCH_ID.split(','))
        b_split_str = "".join("'" + i + "'," for i in SPIR_NO.split(','))
        print('---->', b_split_str.strip(","))
        b_split_str = b_split_str.strip(",")
        s_qry = """ select {} from {} where BATCH_ID in ({}) """.format(colsArray, tableName, b_split_str)
        print(s_qry)
        reference_data = CallingDatabaseTable(accessName, s_qry)
        part_prefix_df = pd.read_excel('V10_ORIG.xls')
        isml_uom = pd.read_excel('uom_file.xlsx')
        print(type(reference_data))
        reference_dataset = REFERENCE.reference_data(reference_data[:50], part_prefix_df, isml_uom)
        print('classAllication_dataset---> ', reference_dataset)
        reference_dataset.to_excel('reference_records.xlsx')
        return_response_data = reference_dataset.to_dict('records')
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
    elif analysisType == 'CHAR_ALLOCATION':
        # start_time = time.time()
        start_time = datetime.now()
        clause_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\Clauses-15.03.22.xlsx", 'Sheet2').iloc[:, :5]
        prop_cndtn_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\Property_Rules.xlsx")
        # prop_cndtn_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\Is_properties_condition_working.xls", "Sheet4").iloc[:, :9]
        # sheet5_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Is_properties_condition_working.xls", "Sheet5")
        desc_cls_file = pd.read_excel(r'C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\desc_replace.xlsx')
        uom_replace_df = pd.read_excel(r"C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\uomv1.xlsx")
        # s_qry_old = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        # s_qry = """ select {} from {} where BATCH_ID in {} """.format(colsArray, tableName, tuple(BATCH_ID.split(',')),
        #                                                               BATCH_ID.split(','))
        b_split_str = "".join("'" + i + "'," for i in SPIR_NO.split(','))
        print('---->', b_split_str.strip(","))
        b_split_str = b_split_str.strip(",")
        s_qry = """ select {} from {} where SPIR_NO in ({}) """.format(colsArray, tableName, b_split_str)
        # s_qry = """ select {} from {} where SPIR_NO in ('{}') """.format(colsArray, tableName, SPIR_NO)
        # s_qry = """ select {} from {} """.format(colsArray, tableName)

        # print(s_qry)
        char_data = CallingDatabaseTable(accessName, s_qry)
        # char_data = pd.read_excel(r'C:\Users\Administrator\PythonDeployment\Class Allocation_New\Server Files\CHAR_EXCELS\2504_3columns.xls').fillna('')
        char_data = char_data[:450]
        print(type(char_data))
        print(char_data)
        # file = pd.ExcelFile('ISML UOM_1.xls')
        # UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
        # UOM1 = pd.read_excel(file, 'uom')
        # charallocation_dataset = CHAR_ALLOCATION_LIVE.char_allocation(clause_df, prop_cndtn_df, char_data[:450], UOM, UOM1, desc_cls_file, sheet5_df)
        charallocation_dataset = CHAR_ALLOCATION_LIVE.char_allocation(clause_df, prop_cndtn_df, char_data[:450], desc_cls_file,uom_replace_df)


        # charallocation_dataset = Char_Final_for_SPIRdata_demo_09_03_22_with_exclude.char_allocation(char_data)
        print('classAllication_dataset---> ', charallocation_dataset)
        if type(charallocation_dataset) != type(pd.DataFrame()):
            print('='*50, type(charallocation_dataset) ,  type(pd.DataFrame()))
            pass
        else:
            # charallocation_dataset.to_excel('reference_records.xlsx')
            return_response_data = charallocation_dataset.to_dict('records')
            # return_response_data = charallocation_dataset
        # charallocation_dataset.to_excel('Char_records.xlsx')
        # return_response_data = charallocation_dataset.to_dict('records')
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
    else:
        return_response_data = """Mr. Data provider, Please check the "analysisType" you have given!"""
    # print('-'*25, 'FINAL RESPONSE', '-'*25, '\n', return_response_data)
    res_dct = {}
    # json_compatible_item_data = jsonable_encoder(return_response_data)
    # return JSONResponse(content=json_compatible_item_data)
    
    res_dct.update({'report': json.dumps(return_response_data, indent=4, cls=json.JSONEncoder)})

    # res_dct.update({'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)})
    # res_dct.update({'report': return_response_data})
    # return JSONResponse(
    #         status_code=200,
    #         content=jsonable_encoder({"detail": res_dct}),
    #     )
    return JSONResponse(content=res_dct)
    # return Response(res_dct)


if __name__ == '__main__':
    uvicorn.run("allocation_caller_TEST:allocationApp", host='172.16.1.60', port=6655, log_level='info')
