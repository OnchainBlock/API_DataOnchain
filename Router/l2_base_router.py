import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

base_router =APIRouter(
    prefix='/Base',
    tags=['Base L2']
)
#statics change in TVL
#statics change in TVL
@base_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Base')


# statistic TVL
@base_router.get('/overview_Base')
async def overview():
    return create_overview_Layer2('Base','base')
#TVL
@base_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Base',start,end)
#Inflow
@base_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Base')
#outflow
@base_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Base')
#transaction daily
@base_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','base',start,end)
#transctiom weekly
@base_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','base',start,end)


