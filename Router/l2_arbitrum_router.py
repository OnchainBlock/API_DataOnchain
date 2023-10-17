import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

arbitrum_router = APIRouter(
    prefix='/arbitrum',
    tags=['Arbitrum L2']
)

#statics change in TVL
@arbitrum_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Arbitrum')


# statistic TVL
@arbitrum_router.get('/overview_arbitrum')
async def overview():
    return create_overview_Layer2('Arbitrum','arbitrum')
#TVL
@arbitrum_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Arbitrum',start,end)
#Inflow
@arbitrum_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Arbitrum')
#outflow
@arbitrum_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Arbitrum')
#transaction daily
@arbitrum_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','arbitrum',start,end)
#transctiom weekly
@arbitrum_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','arbitrum',start,end)

