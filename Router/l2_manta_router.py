import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

manta_router =APIRouter(
    prefix='/Manta',
    tags=['Manta L2']
)
#statics change in TVL
#statics change in TVL
@manta_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Manta')


# statistic TVL
@manta_router.get('/overview_Manta')
async def overview():
    return create_overview_Layer2('Manta','manta')
#TVL
@manta_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Manta',start,end)
#Inflow
@manta_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Manta')
#outflow
@manta_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Manta')
#transaction daily
@manta_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','manta',start,end)
#transctiom weekly
@manta_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','manta',start,end)


