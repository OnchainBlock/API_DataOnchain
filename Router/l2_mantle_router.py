import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

mantle_router =APIRouter(
    prefix='/Mantle',
    tags=['Mantle L2']
)
#statics change in TVL
#statics change in TVL
@mantle_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Mantle')


# statistic TVL
@mantle_router.get('/overview_Mantle')
async def overview():
    return create_overview_Layer2('Mantle','mantle')
#TVL
@mantle_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Mantle',start,end)
#Inflow
@mantle_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Mantle')
#outflow
@mantle_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Mantle')
#transaction daily
@mantle_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','mantle',start,end)
#transctiom weekly
@mantle_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','mantle',start,end)


