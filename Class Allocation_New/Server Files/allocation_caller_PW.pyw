from isk_connectit import DatabaseConnection
import CLASS_ALLOCATION_TEST
import REFERENCE
import CHAR_ALLOCATION
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



class reqDataFormat(BaseModel):
    tableName: str
    colsArray: str
    accessName: str
    BATCH_ID: str
    accessName: str



# async def dataQualifying(tableName: Optional[str], colsArray: Optional[str], accessName: Optional[str], BATCH_ID: Optional[str], analysisType: Optional[str]):
@allocationApp.get("/classallocation")
@allocationApp.post("/classallocation")
async def dataQualifying(tableName: Optional[str] = Form(...), colsArray: Optional[str] = Form(...), accessName: Optional[str] = Form(...), BATCH_ID: Optional[str] = Form(...), analysisType: Optional[str] = Form(...)):

    return_response_data = None
    print('-------------------------- ', analysisType, '-------------------------- ')

    if analysisType == 'CLASS_ALLOCATION':
        # start_time = time.time()
        start_time = datetime.now()

        s_qry = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        print(s_qry)
        data = CallingDatabaseTable(accessName, s_qry)
        print(type(data))
        # classAllication_dataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.class_allocation_func(data[:5])
        classAllication_dataset = CLASS_ALLOCATION_TEST.class_allocation_func(data[:200])

        # unspscDataset = CLASS_AND_UNSPSC_allocation_12_0_01_copy_NO_THREAD.unspsc_allocation_func(classAllication_dataset)
        unspscDataset = CLASS_ALLOCATION_TEST.unspsc_allocation_func(classAllication_dataset)
        unspscDataset.to_excel('Class_records.xlsx')

        # return data.to_dict(orient='records')
        return_response_data = unspscDataset.to_dict('records')
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
    elif analysisType == 'DUMMY_CLASS_ALLOCATION':
        s_qry = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
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
        s_qry = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
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
        clause_df = pd.read_excel("IS_CLAUSES.xls").iloc[:, :5]
        prop_cndtn_df = pd.read_excel("Is_properties_condition_working.xls").iloc[:, :9]
        s_qry = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        print(s_qry)
        char_data = CallingDatabaseTable(accessName, s_qry)
        # char_data = char_data[:500]
        print(type(char_data))
        file = pd.ExcelFile('ISML UOM_1.xls')
        UOM = pd.read_excel(file, 'UOMs TO Be Excluded')
        UOM1 = pd.read_excel(file, 'uom')
        charallocation_dataset = CHAR_ALLOCATION.char_allocation(clause_df, prop_cndtn_df, char_data[:100], UOM, UOM1)
        print('classAllication_dataset---> ', charallocation_dataset)
        charallocation_dataset.to_excel('Char_records.xlsx')
        return_response_data = charallocation_dataset.to_dict('records')
        # print("--- %s seconds ---" % (time.time() - start_time))
        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
    else:
        return_response_data = """Mr. Data provider, Please check the "analysisType" you have given!"""
    # print('-'*25, 'FINAL RESPONSE', '-'*25, '\n', return_response_data)
    res_dct = {}
    res_dct.update({'report': json.dumps(return_response_data, indent=4, cls=NpEncoder)})
    # return JSONResponse(
    #         status_code=200,
    #         content=jsonable_encoder({"detail": res_dct}),
    #     )
    return res_dct


if __name__ == '__main__':
    uvicorn.run("allocation_caller_PW:allocationApp", host='172.16.1.60', port=6648, log_level='info')
