import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

zksync_router = APIRouter(
    prefix='/zksyncEra',
    tags=['Zksync Era L2']
)

@zksync_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('zkSync Era')


# statistic TVL
@zksync_router.get('/overview_starknet')
async def overview():
    return create_overview_Layer2('zkSync Era','zkSync Era')
#TVL
@zksync_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('zkSync Era',start,end)
#Inflow
@zksync_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'zkSync Era')
#outflow
@zksync_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'zkSync Era')
#transaction daily
@zksync_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','zkSync Era',start,end)
#transctiom weekly
@zksync_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','zkSync Era',start,end)