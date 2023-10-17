import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

linea_router =APIRouter(
    prefix='/Linea',
    tags=['Linea L2']
)
#statics change in TVL
#statics change in TVL
@linea_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Linea')


# statistic TVL
@linea_router.get('/overview_Linea')
async def overview():
    return create_overview_Layer2('Linea','linear')
#TVL
@linea_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Linea',start,end)
#Inflow
@linea_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Linea')
#outflow
@linea_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Linea')
#transaction daily
@linea_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','linear',start,end)
#transctiom weekly
@linea_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','linear',start,end)


