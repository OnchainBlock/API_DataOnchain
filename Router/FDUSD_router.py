from imports import *
from FDUSD_data.fdusd import *
fdusd_router = APIRouter(
    prefix='/FDUSD',
    tags=['FDUSD'],

)

@fdusd_router.get('/create_pie')
def pie_df():
    return Funtions_FDUSD.create_pie()

@fdusd_router.get('/create_net_reserves')
def create_df_net_reserves(start:str, end:str): 
    return Funtions_FDUSD.cre_df_netflow(start,end,fdusd)