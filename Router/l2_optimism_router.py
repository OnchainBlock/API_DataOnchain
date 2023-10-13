import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

optimism_router = APIRouter(
    prefix='/Optimism',
    tags=['Optimism L2']
)

@optimism_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Optimism')


# statistic TVL
@optimism_router.get('/overview_Optimism')
async def overview():
    return create_overview_Layer2('Optimism','optimsn')
#TVL
@optimism_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Optimism',start,end)
#Inflow
@optimism_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Optimism')
#outflow
@optimism_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Optimism')
#transaction daily
@optimism_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','optimsn',start,end)
#transctiom weekly
@optimism_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','optimsn',start,end)
