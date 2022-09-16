import RPA_VALIDATION_API_02_03_2022
import PPO_RPA_VALIDATION_03_03_2022
import CLASS5_RPA_VALIDATION_04_03_2022
# importing the libraries
import new_qctools
import uvicorn
from fastapi import FastAPI, Form
from typing import Optional
import sabic_validations
import asyncio

app = FastAPI()


@app.get("/RPA_VALIDATION")
@app.post("/RPA_VALIDATION/")
async def rpa_validation(recordNo: str = Form(...)):
    resp = RPA_VALIDATION_API_02_03_2022.validation_rpa(recordNo)
    return resp


@app.get("/RPA_VALIDATION_BATCH")
@app.post("/RPA_VALIDATION_BATCH/")
async def rpa_validation_batch(orgnid: str = Form(...), BATCH_ID: str = Form(...)):
    # print(request.client)
    await asyncio.sleep(5)
    resp = new_qctools.qc_checks(orgnid, BATCH_ID)
    return resp


@app.get("/SABIC_VALIDATIONS")
@app.post("/SABIC_VALIDATIONS/")
async def dhs(recordNo: str = Form(...), plant: str = Form(...), from_status: str = Form(...),
              to_status: str = Form(...)):
    resp = sabic_validations.sabic_validations(recordNo, plant, from_status, to_status)
    return resp


# @app.get("/PPO_RPA_VALIDATION")
# @app.post("/PPO_RPA_VALIDATION")
# async def rpa_validation(recordNo: str = Form(...)):
#     resp = PPO_RPA_VALIDATION_03_03_2022.validation_rpa_ppo(recordNo)
#     return resp
#
#
# @app.get("/CLASS_DIMENTIONS_RPA_VALIDATION")
# @app.post("/CLASS_DIMENTIONS_RPA_VALIDATION")
# async def rpa_validation(recordNo: str = Form(...)):
#     resp = CLASS5_RPA_VALIDATION_04_03_2022.validation_rpa_dimentions(recordNo)
#     return resp

if __name__ == '__main__':
    uvicorn.run(app, host='172.16.1.62', port=6652)
