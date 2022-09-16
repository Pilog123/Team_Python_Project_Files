import PO_match9
# import RPA_VALIDATION_API_02_03_2022
# import PPO_RPA_VALIDATION_03_03_2022
# importing the libraries
import uvicorn
from fastapi import FastAPI, Form, Depends
from typing import Optional
from fastapi.responses import JSONResponse
import numpy as np
import pandas as pd
import cx_Oracle


from fastapi import FastAPI, Form, Body, BackgroundTasks
# from random_process_new import score_calculator


app = FastAPI()


@app.get('/POMATCH')
@app.post('/POMATCH')
async def main(tableName: Optional[str] = Form(None),colsArray: Optional[str] = Form(None), accessName: Optional[str] = Form(None), BATCH_ID: Optional[str] = Form(None), analysisType: Optional[str] = Form(None)):
    # resp = None
    req_dct = {}
    if analysisType == 'POMATCH':
        BATCH_ID = BATCH_ID.replace(',', "','").replace(' ', '')

        conn = cx_Oracle.connect('DR1024213/Pipl#mdrm$213@172.16.1.61:1521/DR101413')
        # s_qry = """ select {} from {} where BATCH_ID = '{}' """.format(colsArray, tableName, BATCH_ID)
        s_qry = """ select {} from {} where BATCH_ID in ('{}') """.format(colsArray, tableName, BATCH_ID)
        data = pd.read_sql(s_qry, conn)
        conn.close()


        # data = pd.read_excel('DH_Match_PO.xlsx', 'New data')
        # print(s_qry)
        print(type(data))
        print(data)
        req_dct.update({'tableName':tableName})
        req_dct.update({'colsArray':colsArray})
        req_dct.update({'accessName':accessName})
        req_dct.update({'BATCH_ID':BATCH_ID})
        req_dct.update({'analysisType':analysisType})
        resp=PO_match9.po_percentage(req_dct, data[:500], colsArray, tableName, BATCH_ID)

        # resp=PO_match9.po_percentage(req_dct, data)
        # print('------', '\n', resp, '\n', '------')
        # resp.to_excel('po_sample_output_16.03.2022.xlsx', index=False)
        return {'report':resp}


# @app.get("/CLASS_RPA_VALIDATION")
# @app.post("/CLASS_RPA_VALIDATION")
# async def rpa_validation(recordNo: str = Form(...)):

#     resp = RPA_VALIDATION_API_02_03_2022.validation_rpa(recordNo)
#     return resp


# @app.get("/PPO_RPA_VALIDATION")
# @app.post("/PPO_RPA_VALIDATION")
# async def rpa_validation(recordNo: str = Form(...)):
#     resp = PPO_RPA_VALIDATION_03_03_2022.validation_rpa_ppo(recordNo)
#     return resp

#-----------------------------------------------------------------------------------------------------------------------

# def Update_Qry(rdf, uqy):
#     print('UPDATE CALLED and waiting...')
#     # time.sleep(10)
#     print('Waiting Done')
#     print('Started updating...')
#     conn = cx_Oracle.connect('DR1024193/Pipl#mdrm$93@172.16.1.61:1521/DR101412')
#     cur = conn.cursor()
#     # ==================================================
#     # Insert DataFrame recrds one by one.
#     qc = 0
#     # print(rdf)
#     # print(uqy)
#     # print(rdf.columns)

#     for i, row in rdf.iterrows():
#         qc += 1
#         #print(row)
#         cur.execute(uqy, tuple(row))
#         conn.commit()


#     conn.close()
#     print('Done!')


# analysisType_Dict = {'BSNS_IMPCT_AND_INVST_AND_SCORE': ['tableName' , 'colsArray' , 'BATCH_ID' , 'analysisType' , 'BUSINESS_CRITICAL' , 'BUSINESS_INVEST_VH_TO' , 'BUSINESS_IMPACT_VH_FROM' , 'BUSINESS_IMPACT_L' , 'BUSINESS_IMPACT_M_TO' , 'BUSINESS_SCORE_M_FROM' ,   'BUSINESS_SCORE_L' , 'BUSINESS_IMPACT' ,  'BUSINESS_INVEST_M_FROM' , 'BUSINESS_SCORE_VH_TO' ,  'BUSINESS_INVEST_H_TO' , 'BUSINESS_IMPACT_H_TO' , 'BUSINESS_SCORE_M_TO' , 'BUSINESS_IMPACT_M_FROM' , 'BUSINESS_SCORE_VH_FROM' , 'BUSINESS_INVEST' , 'BUSINESS_INVEST_H_FROM' , 'BUSINESS_SCORE_H_FROM' , 'BUSINESS_SCORE_H_TO' ,  'BUSINESS_IMPACT_VH_TO' , 'BUSINESS_INVEST_L' , 'BUSINESS_INVEST_M_TO' , 'BUSINESS_SCORE' , 'BUSINESS_INVEST_VH_FROM' ,  'BUSINESS_IMPACT_H_FROM'],
#                     'MTNC_RDC_AND_YMC_AND_MF_AND_MWO_AND_MnTB':  ['tableName' , 'colsArray' , 'BATCH_ID' , 'analysisType' , 'MAINTAINABLE', 'REDUNDANCY_LINE', 'REDUNDANCY_LINE_H_FROM', 'REDUNDANCY_LINE_H_TO', 'REDUNDANCY_LINE_L', 'REDUNDANCY_LINE_M_FROM', 'REDUNDANCY_LINE_M_TO', 'REDUNDANCY_LINE_VH_FROM', 'REDUNDANCY_LINE_VH_TO', 'YEARLY_MAINTENANCE_COST', 'YEARLY_MAINTENANCE_COST_H_FROM', 'YEARLY_MAINTENANCE_COST_H_TO', 'YEARLY_MAINTENANCE_COST_L', 'YEARLY_MAINTENANCE_COST_M_FROM', 'YEARLY_MAINTENANCE_COST_M_TO', 'YEARLY_MAINTENANCE_COST_VH_FROM', 'YEARLY_MAINTENANCE_COST_VH_TO', 'MAINTENANCE_FREQUENCY', 'MAINTENANCE_FREQUENCY_H_FROM', 'MAINTENANCE_FREQUENCY_H_TO', 'MAINTENANCE_FREQUENCY_L', 'MAINTENANCE_FREQUENCY_M_FROM', 'MAINTENANCE_FREQUENCY_M_TO', 'MAINTENANCE_FREQUENCY_VH_FROM', 'MAINTENANCE_FREQUENCY_VH_TO', 'MAINTENANCE_WORK_ORDERS', 'MAINTENANCE_WORK_ORDERS_VH_FROM', 'MAINTENANCE_WORK_ORDERS_VH_TO', 'MAINTENANCE_WORK_ORDERS_H_FROM', 'MAINTENANCE_WORK_ORDERS_H_TO', 'MAINTENANCE_WORK_ORDERS_L', 'MAINTENANCE_WORK_ORDERS_M_FROM', 'MAINTENANCE_WORK_ORDERS_M_TO', 'MEAN_TIME_BEFORE_FAILURE', 'MEAN_TIME_BEFORE_FAILURE_H_FROM', 'MEAN_TIME_BEFORE_FAILURE_H_TO', 'MEAN_TIME_BEFORE_FAILURE_M_FROM', 'MEAN_TIME_BEFORE_FAILURE_M_TO', 'MEAN_TIME_BEFORE_FAILURE_VH_FROM', 'MEAN_TIME_BEFORE_FAILURE_VH_TO', 'MEAN_TIME_BEFORE_FAILURE_L', 'MAINTENANCE_SCORE', 'MAINTENANCE_SCORE_VH_FROM', 'MAINTENANCE_SCORE_VH_TO', 'MAINTENANCE_SCORE_H_FROM', 'MAINTENANCE_SCORE_H_TO', 'MAINTENANCE_SCORE_M_FROM', 'MAINTENANCE_SCORE_M_TO', 'MAINTENANCE_SCORE_L'],
#                     'SFTC_HS_AND_AS_AND_SSA':  ['tableName', 'BATCH_ID', 'colsArray', 'analysisType', 'SAFETY_SCORE_VVH', 'SAFETY_SCORE_VH_FROM', 'SAFETY_SCORE_VH_TO', 'SAFETY_SCORE_H_FROM', 'SAFETY_SCORE_H_TO', 'SAFETY_SCORE_M_FROM', 'SAFETY_SCORE_M_TO',  'SAFETY_SCORE_L'],
#                     'PRD_IMP': ['tableName', 'BATCH_ID', 'colsArray', 'analysisType'],
#                     'FMSN': ['F', 'S', 'tableName', 'colsArray', 'analysisType', 'MATERIAL_TYPE', 'BATCH_ID', 'PLANT', 'PURCHASING_GROUP', 'STORAGE_LOCATION', 'MATERIAL_GROUP', 'ISIC', 'UNSPSC', 'INSTANCE', 'INDUSTRY'],
#                     'VED':  ['V', 'D', 'tableName', 'colsArray', 'analysisType', 'MATERIAL_TYPE', 'BATCH_ID', 'PLANT', 'PURCHASING_GROUP', 'STORAGE_LOCATION', 'MATERIAL_GROUP', 'ISIC', 'UNSPSC', 'INSTANCE', 'INDUSTRY'],
#                     'SDE':  ['S', 'E', 'tableName', 'colsArray', 'analysisType', 'MATERIAL_TYPE', 'BATCH_ID', 'PLANT', 'PURCHASING_GROUP', 'STORAGE_LOCATION', 'MATERIAL_GROUP', 'ISIC', 'UNSPSC', 'INSTANCE', 'INDUSTRY']
#                             }

# @app.get('/ECA')
# @app.post('/ECA')
# # def MP(background_tasks: BackgroundTasks, tableName : str = Form(...), colsArray : str = Form(...), BATCH_ID : str = Form(...), analysisType : str = Form(...), BUSINESS_INVEST_VH_TO : str = Form(...), BUSINESS_CRITICAL : str = Form(...), BUSINESS_IMPACT_VH_FROM : str = Form(...), BUSINESS_IMPACT_L : str = Form(...), BUSINESS_IMPACT_M_TO : str = Form(...), BUSINESS_SCORE_M_FROM : Optional[str] = None,   BUSINESS_SCORE_L : Optional[str] = None, BUSINESS_IMPACT : str = Form(...),  BUSINESS_INVEST_M_FROM : str = Form(...), BUSINESS_SCORE_VH_TO : Optional[str] = None,  BUSINESS_INVEST_H_TO : str = Form(...), BUSINESS_IMPACT_H_TO : str = Form(...), BUSINESS_SCORE_M_TO : Optional[str] = None, BUSINESS_IMPACT_M_FROM : str = Form(...), BUSINESS_SCORE_VH_FROM : Optional[str] = None, BUSINESS_INVEST : str = Form(...), BUSINESS_INVEST_H_FROM : str = Form(...), BUSINESS_SCORE_H_FROM : Optional[str] = None, BUSINESS_SCORE_H_TO : Optional[str] = None,  BUSINESS_IMPACT_VH_TO : str = Form(...), BUSINESS_INVEST_L : str = Form(...), BUSINESS_INVEST_M_TO : str = Form(...), BUSINESS_SCORE : Optional[str] = None, BUSINESS_INVEST_VH_FROM : str = Form(...),  BUSINESS_IMPACT_H_FROM : str = Form(...), REDUNDANCY_LINE : str = Form(...), REDUNDANCY_LINE_H_FROM : str = Form(...), REDUNDANCY_LINE_H_TO : str = Form(...), REDUNDANCY_LINE_L : str = Form(...), REDUNDANCY_LINE_M_FROM : str = Form(...), REDUNDANCY_LINE_M_TO : str = Form(...), REDUNDANCY_LINE_VH_FROM : str = Form(...), REDUNDANCY_LINE_VH_TO : str = Form(...), YEARLY_MAINTENANCE_COST : str = Form(...), YEARLY_MAINTENANCE_COST_H_FROM : str = Form(...), YEARLY_MAINTENANCE_COST_H_TO : str = Form(...), YEARLY_MAINTENANCE_COST_L : str = Form(...), YEARLY_MAINTENANCE_COST_M_FROM : str = Form(...), YEARLY_MAINTENANCE_COST_M_TO : str = Form(...), YEARLY_MAINTENANCE_COST_VH_FROM : str = Form(...), YEARLY_MAINTENANCE_COST_VH_TO : str = Form(...), MAINTENANCE_FREQUENCY : str = Form(...), MAINTENANCE_FREQUENCY_H_FROM : str = Form(...), MAINTENANCE_FREQUENCY_H_TO : str = Form(...), MAINTENANCE_FREQUENCY_L : str = Form(...), MAINTENANCE_FREQUENCY_M_FROM : str = Form(...), MAINTENANCE_FREQUENCY_M_TO : str = Form(...), MAINTENANCE_FREQUENCY_VH_FROM : str = Form(...), MAINTENANCE_FREQUENCY_VH_TO : str = Form(...), MAINTENANCE_WORK_ORDERS : str = Form(...), MAINTENANCE_WORK_ORDERS_VH_FROM : str = Form(...), MAINTENANCE_WORK_ORDERS_VH_TO : str = Form(...), MAINTENANCE_WORK_ORDERS_H_FROM : str = Form(...), MAINTENANCE_WORK_ORDERS_H_TO : str = Form(...), MAINTENANCE_WORK_ORDERS_L : str = Form(...), MAINTENANCE_WORK_ORDERS_M_FROM : str = Form(...), MAINTENANCE_WORK_ORDERS_M_TO : str = Form(...), MEAN_TIME_BEFORE_FAILURE : str = Form(...), MEAN_TIME_BEFORE_FAILURE_H_FROM : str = Form(...), MEAN_TIME_BEFORE_FAILURE_H_TO : str = Form(...), MEAN_TIME_BEFORE_FAILURE_M_FROM : str = Form(...), MEAN_TIME_BEFORE_FAILURE_M_TO : str = Form(...), MEAN_TIME_BEFORE_FAILURE_VH_FROM : str = Form(...), MEAN_TIME_BEFORE_FAILURE_VH_TO : str = Form(...), MEAN_TIME_BEFORE_FAILURE_L : str = Form(...), MAINTAINABLE : str = Form(...)):
# def MP(background_tasks: BackgroundTasks, tableName: Optional[str] = Form(None), colsArray: Optional[str] = Form(None),
#        BATCH_ID: Optional[str] = Form(None), analysisType: Optional[str] = Form(None),
#        BUSINESS_INVEST_VH_TO: Optional[str] = Form(None), BUSINESS_CRITICAL: Optional[str] = Form(None),
#        BUSINESS_IMPACT_VH_FROM: Optional[str] = Form(None), BUSINESS_IMPACT_L: Optional[str] = Form(None),
#        BUSINESS_IMPACT_M_TO: Optional[str] = Form(None), BUSINESS_SCORE_M_FROM: Optional[str] = Form(None),
#        BUSINESS_SCORE_L: Optional[str] = Form(None), BUSINESS_IMPACT: Optional[str] = Form(None),
#        BUSINESS_INVEST_M_FROM: Optional[str] = Form(None), BUSINESS_SCORE_VH_TO: Optional[str] = Form(None),
#        BUSINESS_INVEST_H_TO: Optional[str] = Form(None), BUSINESS_IMPACT_H_TO: Optional[str] = Form(None),
#        BUSINESS_SCORE_M_TO: Optional[str] = Form(None), BUSINESS_IMPACT_M_FROM: Optional[str] = Form(None),
#        BUSINESS_SCORE_VH_FROM: Optional[str] = Form(None), BUSINESS_INVEST: Optional[str] = Form(None),
#        BUSINESS_INVEST_H_FROM: Optional[str] = Form(None), BUSINESS_SCORE_H_FROM: Optional[str] = Form(None),
#        BUSINESS_SCORE_H_TO: Optional[str] = Form(None), BUSINESS_IMPACT_VH_TO: Optional[str] = Form(None),
#        BUSINESS_INVEST_L: Optional[str] = Form(None), BUSINESS_INVEST_M_TO: Optional[str] = Form(None),
#        BUSINESS_SCORE: Optional[str] = Form(None), BUSINESS_INVEST_VH_FROM: Optional[str] = Form(None),
#        BUSINESS_IMPACT_H_FROM: Optional[str] = Form(None), REDUNDANCY_LINE: Optional[str] = Form(None),
#        REDUNDANCY_LINE_H_FROM: Optional[str] = Form(None), REDUNDANCY_LINE_H_TO: Optional[str] = Form(None),
#        REDUNDANCY_LINE_L: Optional[str] = Form(None), REDUNDANCY_LINE_M_FROM: Optional[str] = Form(None),
#        REDUNDANCY_LINE_M_TO: Optional[str] = Form(None), REDUNDANCY_LINE_VH_FROM: Optional[str] = Form(None),
#        REDUNDANCY_LINE_VH_TO: Optional[str] = Form(None), YEARLY_MAINTENANCE_COST: Optional[str] = Form(None),
#        YEARLY_MAINTENANCE_COST_H_FROM: Optional[str] = Form(None),
#        YEARLY_MAINTENANCE_COST_H_TO: Optional[str] = Form(None), YEARLY_MAINTENANCE_COST_L: Optional[str] = Form(None),
#        YEARLY_MAINTENANCE_COST_M_FROM: Optional[str] = Form(None),
#        YEARLY_MAINTENANCE_COST_M_TO: Optional[str] = Form(None),
#        YEARLY_MAINTENANCE_COST_VH_FROM: Optional[str] = Form(None),
#        YEARLY_MAINTENANCE_COST_VH_TO: Optional[str] = Form(None), MAINTENANCE_FREQUENCY: Optional[str] = Form(None),
#        MAINTENANCE_FREQUENCY_H_FROM: Optional[str] = Form(None), MAINTENANCE_FREQUENCY_H_TO: Optional[str] = Form(None),
#        MAINTENANCE_FREQUENCY_L: Optional[str] = Form(None), MAINTENANCE_FREQUENCY_M_FROM: Optional[str] = Form(None),
#        MAINTENANCE_FREQUENCY_M_TO: Optional[str] = Form(None),
#        MAINTENANCE_FREQUENCY_VH_FROM: Optional[str] = Form(None),
#        MAINTENANCE_FREQUENCY_VH_TO: Optional[str] = Form(None), MAINTENANCE_WORK_ORDERS: Optional[str] = Form(None),
#        MAINTENANCE_WORK_ORDERS_VH_FROM: Optional[str] = Form(None),
#        MAINTENANCE_WORK_ORDERS_VH_TO: Optional[str] = Form(None),
#        MAINTENANCE_WORK_ORDERS_H_FROM: Optional[str] = Form(None),
#        MAINTENANCE_WORK_ORDERS_H_TO: Optional[str] = Form(None), MAINTENANCE_WORK_ORDERS_L: Optional[str] = Form(None),
#        MAINTENANCE_WORK_ORDERS_M_FROM: Optional[str] = Form(None),
#        MAINTENANCE_WORK_ORDERS_M_TO: Optional[str] = Form(None), MEAN_TIME_BEFORE_FAILURE: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_H_FROM: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_H_TO: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_M_FROM: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_M_TO: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_VH_FROM: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_VH_TO: Optional[str] = Form(None),
#        MEAN_TIME_BEFORE_FAILURE_L: Optional[str] = Form(None), MAINTAINABLE: Optional[str] = Form(None),
#        MAINTENANCE_SCORE: Optional[str] = Form(None), MAINTENANCE_SCORE_VH_FROM: Optional[str] = Form(None),
#        MAINTENANCE_SCORE_VH_TO: Optional[str] = Form(None), MAINTENANCE_SCORE_H_FROM: Optional[str] = Form(None),
#        MAINTENANCE_SCORE_H_TO: Optional[str] = Form(None), MAINTENANCE_SCORE_M_FROM: Optional[str] = Form(None),
#        MAINTENANCE_SCORE_M_TO: Optional[str] = Form(None), MAINTENANCE_SCORE_L: Optional[str] = Form(None),
#        SAFETY_SCORE_VVH: Optional[str] = Form(None), SAFETY_SCORE_VH_FROM: Optional[str] = Form(None),
#        SAFETY_SCORE_VH_TO: Optional[str] = Form(None), SAFETY_SCORE_H_FROM: Optional[str] = Form(None),
#        SAFETY_SCORE_H_TO: Optional[str] = Form(None), SAFETY_SCORE_M_FROM: Optional[str] = Form(None),
#        SAFETY_SCORE_M_TO: Optional[str] = Form(None), SAFETY_SCORE_L: Optional[str] = Form(None)):
#     req_dct = {
#         "BSNS_IMPCT_AND_INVST_AND_SCORE": [tableName, BATCH_ID, colsArray, analysisType, BUSINESS_IMPACT,
#                                            BUSINESS_IMPACT_VH_FROM, BUSINESS_IMPACT_VH_TO, BUSINESS_IMPACT_H_FROM,
#                                            BUSINESS_IMPACT_H_TO, BUSINESS_IMPACT_M_FROM, BUSINESS_IMPACT_M_TO,
#                                            BUSINESS_IMPACT_L, BUSINESS_INVEST, BUSINESS_INVEST_VH_FROM,
#                                            BUSINESS_INVEST_VH_TO, BUSINESS_INVEST_H_FROM, BUSINESS_INVEST_H_TO,
#                                            BUSINESS_INVEST_M_FROM, BUSINESS_INVEST_M_TO, BUSINESS_INVEST_L,
#                                            BUSINESS_SCORE, BUSINESS_SCORE_VH_FROM, BUSINESS_SCORE_VH_TO,
#                                            BUSINESS_SCORE_H_FROM, BUSINESS_SCORE_H_TO, BUSINESS_SCORE_M_FROM,
#                                            BUSINESS_SCORE_M_TO, BUSINESS_SCORE_L, BUSINESS_CRITICAL],
#         "MTNC_RDC_AND_YMC_AND_MF_AND_MWO_AND_MnTB": [tableName, BATCH_ID, colsArray, analysisType, REDUNDANCY_LINE,
#                                                      REDUNDANCY_LINE_H_FROM, REDUNDANCY_LINE_H_TO, REDUNDANCY_LINE_L,
#                                                      REDUNDANCY_LINE_M_FROM, REDUNDANCY_LINE_M_TO,
#                                                      REDUNDANCY_LINE_VH_FROM, REDUNDANCY_LINE_VH_TO,
#                                                      YEARLY_MAINTENANCE_COST, YEARLY_MAINTENANCE_COST_H_FROM,
#                                                      YEARLY_MAINTENANCE_COST_H_TO, YEARLY_MAINTENANCE_COST_L,
#                                                      YEARLY_MAINTENANCE_COST_M_FROM, YEARLY_MAINTENANCE_COST_M_TO,
#                                                      YEARLY_MAINTENANCE_COST_VH_FROM, YEARLY_MAINTENANCE_COST_VH_TO,
#                                                      MAINTENANCE_FREQUENCY, MAINTENANCE_FREQUENCY_H_FROM,
#                                                      MAINTENANCE_FREQUENCY_H_TO, MAINTENANCE_FREQUENCY_L,
#                                                      MAINTENANCE_FREQUENCY_M_FROM, MAINTENANCE_FREQUENCY_M_TO,
#                                                      MAINTENANCE_FREQUENCY_VH_FROM, MAINTENANCE_FREQUENCY_VH_TO,
#                                                      MAINTENANCE_WORK_ORDERS, MAINTENANCE_WORK_ORDERS_VH_FROM,
#                                                      MAINTENANCE_WORK_ORDERS_VH_TO, MAINTENANCE_WORK_ORDERS_H_FROM,
#                                                      MAINTENANCE_WORK_ORDERS_H_TO, MAINTENANCE_WORK_ORDERS_L,
#                                                      MAINTENANCE_WORK_ORDERS_M_FROM, MAINTENANCE_WORK_ORDERS_M_TO,
#                                                      MEAN_TIME_BEFORE_FAILURE, MEAN_TIME_BEFORE_FAILURE_H_FROM,
#                                                      MEAN_TIME_BEFORE_FAILURE_H_TO, MEAN_TIME_BEFORE_FAILURE_M_FROM,
#                                                      MEAN_TIME_BEFORE_FAILURE_M_TO, MEAN_TIME_BEFORE_FAILURE_VH_FROM,
#                                                      MEAN_TIME_BEFORE_FAILURE_VH_TO, MEAN_TIME_BEFORE_FAILURE_L,
#                                                      MAINTENANCE_SCORE, MAINTENANCE_SCORE_VH_FROM,
#                                                      MAINTENANCE_SCORE_VH_TO, MAINTENANCE_SCORE_H_FROM,
#                                                      MAINTENANCE_SCORE_H_TO, MAINTENANCE_SCORE_M_FROM,
#                                                      MAINTENANCE_SCORE_M_TO, MAINTENANCE_SCORE_L],
#         "SFTC_HS_AND_AS_AND_SSA": [tableName, BATCH_ID, colsArray, analysisType, SAFETY_SCORE_VVH, SAFETY_SCORE_VH_FROM,
#                                    SAFETY_SCORE_VH_TO, SAFETY_SCORE_H_FROM, SAFETY_SCORE_H_TO, SAFETY_SCORE_M_FROM,
#                                    SAFETY_SCORE_M_TO, SAFETY_SCORE_L],
#         "PRD_IMP": [tableName, BATCH_ID, colsArray, analysisType]
#     }
#     # print(BATCH_ID, BUSINESS_INVEST_VH_TO, BUSINESS_CRITICAL, '\n', '='*100, '\n')
#     # print(tableName, BATCH_ID, colsArray, BUSINESS_CRITICAL, analysisType, BUSINESS_IMPACT,   BUSINESS_IMPACT_H_FROM,   BUSINESS_IMPACT_H_TO,   BUSINESS_IMPACT_VH_FROM,   BUSINESS_IMPACT_VH_TO,   BUSINESS_IMPACT_M_FROM,   BUSINESS_IMPACT_M_TO,   BUSINESS_IMPACT_L, BUSINESS_INVEST,   BUSINESS_INVEST_H_FROM,   BUSINESS_INVEST_H_TO,   BUSINESS_INVEST_VH_FROM,   BUSINESS_INVEST_VH_TO,   BUSINESS_INVEST_M_FROM,   BUSINESS_INVEST_M_TO,   BUSINESS_INVEST_L, REDUNDANCY_LINE, REDUNDANCY_LINE_H_FROM, REDUNDANCY_LINE_H_TO, REDUNDANCY_LINE_L, REDUNDANCY_LINE_M_FROM, REDUNDANCY_LINE_M_TO, REDUNDANCY_LINE_VH_FROM, REDUNDANCY_LINE_VH_TO, YEARLY_MAINTENANCE_COST, YEARLY_MAINTENANCE_COST_H_FROM, YEARLY_MAINTENANCE_COST_H_TO, YEARLY_MAINTENANCE_COST_L, YEARLY_MAINTENANCE_COST_M_FROM, YEARLY_MAINTENANCE_COST_M_TO, YEARLY_MAINTENANCE_COST_VH_FROM, YEARLY_MAINTENANCE_COST_VH_TO, MAINTENANCE_FREQUENCY, MAINTENANCE_FREQUENCY_H_FROM, MAINTENANCE_FREQUENCY_H_TO, MAINTENANCE_FREQUENCY_L, MAINTENANCE_FREQUENCY_M_FROM, MAINTENANCE_FREQUENCY_M_TO, MAINTENANCE_FREQUENCY_VH_FROM, MAINTENANCE_FREQUENCY_VH_TO, MAINTENANCE_WORK_ORDERS, MAINTENANCE_WORK_ORDERS_VH_FROM, MAINTENANCE_WORK_ORDERS_VH_TO, MAINTENANCE_WORK_ORDERS_H_FROM, MAINTENANCE_WORK_ORDERS_H_TO, MAINTENANCE_WORK_ORDERS_L, MAINTENANCE_WORK_ORDERS_M_FROM, MAINTENANCE_WORK_ORDERS_M_TO, MEAN_TIME_BEFORE_FAILURE, MEAN_TIME_BEFORE_FAILURE_H_FROM, MEAN_TIME_BEFORE_FAILURE_H_TO, MEAN_TIME_BEFORE_FAILURE_M_FROM, MEAN_TIME_BEFORE_FAILURE_M_TO, MEAN_TIME_BEFORE_FAILURE_VH_FROM, MEAN_TIME_BEFORE_FAILURE_VH_TO, MEAN_TIME_BEFORE_FAILURE_L, MAINTAINABLE)

#     rqd = {}
#     if analysisType in list(analysisType_Dict.keys()):
#         rqk_l = analysisType_Dict.get(analysisType)
#         for rqk in rqk_l:
#             rqd.update({'{}'.format(rqk): eval(rqk)})
#             # rqd['{}'.format(rqk)] = eval(rqk)
#     print('~~~~~~~~~~~~~~~', rqd, '~~~~~~~~~~~~~~~~~')

#     print('\n')
#     for k, v in rqd.items():
#         print(k, '--> ', v, end='\n')
#     print('\n')

#     out_put = score_calculator(rqd)
#     print('DONE!')
#     background_tasks.add_task(Update_Qry, out_put['rdf'], out_put['uqy'])
#     print('=' * 60, '\n', out_put['res_dct'], '\n', '=' * 60)
#     return out_put['res_dct']






#-----------------------------------------------------------------------------------------------------------------------



if __name__ == '__main__':
    uvicorn.run(app, host='172.16.1.62', port=6654)
