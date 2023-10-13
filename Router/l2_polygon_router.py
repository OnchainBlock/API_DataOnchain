import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

polygon_router = APIRouter(
    prefix='/Polygon',
    tags=['Polygon L2']
)

@polygon_router.get('/statistic_time')
async def change_time():
    return Funtions_TVL.create_table('Polygon')


# statistic TVL
@polygon_router.get('/overview_Polygon')
async def overview():
    return create_overview_Layer2('Polygon','polygon')
#TVL
@polygon_router.get('/TVL_dataframe')
async def tvl(start:str,end:str):
    return Funtions_TVL.create_bridge('Polygon',start,end)
#Inflow
@polygon_router.get('/Inflow')
async def Inflow(start:str,end:str):
    return Funtions_TVL.Inflow_layer2(start,end,'Polygon')
#outflow
@polygon_router.get('/outflow')
async def Outflow(start:str, end:str):
    return Funtions_TVL.OutFlow(start,end,'Polygon')
#transaction daily
@polygon_router.get('/daily')
async def daily(start:str,end:str):
    return tx_layer2_time('daily','polygon',start,end)
#transctiom weekly
@polygon_router.get('/weekly')
async def weekly(start:str,end:str):
    return tx_layer2_time('weekly','polygon',start,end)


