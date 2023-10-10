import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *

arbitrum_router = APIRouter(
    prefix='/arbitrum',
    tags=['Arbitrum L2']
)
@arbitrum_router.get('/dataframe')
async def choice_df(start:str,end:str):
    return create_dataframe.choice_l2('arbitrum',start,end)