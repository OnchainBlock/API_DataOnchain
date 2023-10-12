import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.L2_overviews import *
l2_tx_router = APIRouter(
    prefix='/Overview_l2',
    tags=['Layer2 Transaction']
)

@l2_tx_router.get('/statics_l2')
async def statistics_l2(l2:str):
    return create_statics_L2(l2)

@l2_tx_router.get('/unique_users/Daily')
async def Daily(l2:str,start:str,end:str):
    return Func_Layer2.Daily(l2,start,end,'unique_users')

@l2_tx_router.get('/amount_eth/Daily')
async def Daily(l2:str,start:str,end:str):
    return Func_Layer2.Daily(l2,start,end,'eth_amount')

@l2_tx_router.get('/tx/Daily')
async def Daily(l2:str,start:str,end:str):
    return Func_Layer2.Daily(l2,start,end,'tx')

@l2_tx_router.get('/fee/Daily')
async def Daily(l2:str,start:str,end:str):
    return Func_Layer2.Daily(l2,start,end,'fee_tx')

# Weekly
@l2_tx_router.get('/unique_users/Weekly')
async def Weekly(l2:str,start:str,end:str):
    return Func_Layer2.Weekly(l2,start,end,'unique_users')

@l2_tx_router.get('/amount_eth/Weekly')
async def Weekly(l2:str,start:str,end:str):
    return Func_Layer2.Weekly(l2,start,end,'eth_amount')

@l2_tx_router.get('/tx/Weekly')
async def Weekly(l2:str,start:str,end:str):
    return Func_Layer2.Weekly(l2,start,end,'tx')

@l2_tx_router.get('/fee/Weekly')
async def Weekly(l2:str,start:str,end:str):
    return Func_Layer2.Weekly(l2,start,end,'fee_tx')

@l2_tx_router.get('/table')
async def table():
    return create_table_overview()
@l2_tx_router.get('/treemap')
def Treemap():
    return treemap()