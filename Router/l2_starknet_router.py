import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

starknet_router =APIRouter(
    prefix='/starknet',
    tags=['Starknet L2']
)
#statics change in TVL
#statics change in TVL
@starknet_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('StarkNet')


# statistic TVL
@starknet_router.get('/overview_starknet')
async def overview():
    return create_overview_Layer2('StarkNet','StarkNet')
#TVL
@starknet_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('StarkNet',start,end)
#Inflow
@starknet_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(TVL_df,start,end,'StarkNet')
#outflow
@starknet_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(TVL_df,start,end,'StarkNet')
#transaction daily
@starknet_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','StarkNet',start,end)
#transctiom weekly
@starknet_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','StarkNet',start,end)


