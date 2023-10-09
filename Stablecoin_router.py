import sys
sys.path.append(r'D:\DATA\GIT\API_DataOnchain')
from imports import *
from Stablecoin_V1_data.Stablecoin_v1 import *
stablecoin_v1_router = APIRouter(
    prefix='/Stablecoin_v1',
    tags=['Stablecoin v1']
)

@stablecoin_v1_router.get('/pie')
async def create_pie_df(token:str):
    choice_condition = ['all','usdt','usdc','busd','dai','lusd','tusd']
    if token not in choice_condition:
        return f'balance: {token} is not found, plase choice another ["all","usdt","usdc","busd","dai","lusd","tusd"]'
    
    elif token =='all':
        data = now_token.groupby(['balance']).agg({'value':'sum'}).reset_index().sort_values(by=['value'], ascending=False).to_dict(orient='records')
    else:
        data = now_token[now_token['token']==token].sort_values(by=['value'],ascending=False).to_dict(orient='records')
    return data

@stablecoin_v1_router.get('/balance')
async def create_balance_data(balance:str,token_choice:str, start:str, end:str):
    return create_line_df(balance,token_choice,start,end)