import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

Scroll_router =APIRouter(
    prefix='/Scroll',
    tags=['Scroll L2']
)
#statics change in TVL
#statics change in TVL
@Scroll_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Scroll')


# statistic TVL
@Scroll_router.get('/overview_Scroll')
async def overview():
    return create_overview_Layer2('Scroll','scroll')
#TVL
@Scroll_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Scroll',start,end)
#Inflow
@Scroll_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Scroll')
#outflow
@Scroll_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Scroll')
#transaction daily
@Scroll_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','scroll',start,end)
#transctiom weekly
@Scroll_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','scroll',start,end)



