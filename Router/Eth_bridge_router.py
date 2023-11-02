import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
from Layer2_data.Storage_Data_L2 import *


eth_bridge_router = APIRouter(
    prefix='/bridge',
    tags=['ETH'],

)

my_server = os.environ['my_server']
query_ETH = os.environ['query_ETH_bridge']

eth_bridge = pd.read_sql(query_ETH,my_server)
# config format time
eth_bridge['time'] = eth_bridge['time'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
# config format time
@eth_bridge_router.get('/pie')
async def pie():
    df_pie = eth_bridge.loc[eth_bridge['time'] == eth_bridge['time'].max()][['value','bridge']].rename(columns={'value':'VALUE','bridge':'EXPLORER'})
    df_pie = df_pie.sort_values(by=['VALUE'],ascending=False)
    return df_pie.to_dict(orient='records')

@eth_bridge_router.get('/balance')
async def bridge_ETH(bridge:str,start:str,end:str):
     
    choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon ZKEVM','Linea', 'Base','Mantle','Manta','Scroll']
    if bridge not in choice_condition:
        return f'balance: {bridge} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon ZKEVM","Linea", "Base","Mantle","Manta","Scroll"]'
    elif bridge=="Arbitrum":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Optimism":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="zkSync Era":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="StarkNet":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Polygon ZKEVM":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Linea":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Base":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Mantle":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Manta":
        return Funtions_TVL.create_bridge(bridge,start,end)
    elif bridge=="Scroll":
        return Funtions_TVL.create_bridge(bridge,start,end)

# NEtflow api in here
@eth_bridge_router.get('/Netflow')
async def func_netflow(start:str,end:str) -> None:
    return Funtions_TVL.create_netflow_df(eth_bridge,start,end)
        

# API table
@eth_bridge_router.get('/table')
async def create_table()->None:
        return Funtions_TVL.create_table()
    
#create Inflow / OutFlow Ethereum in layers2

@eth_bridge_router.get('/Inflow_layer2')
async def Inflow_layer2(start:str,end:str,label:str):
     return Funtions_TVL.Inflow_layer2(eth_bridge,start,end,label)
        
    
@eth_bridge_router.get('/Outflow_layer2')
async def Outflow_layer2(start:str,end:str,label:str):
    return Funtions_TVL.OutFlow(eth_bridge,start,end,label)