from imports import *
import pandas as pd


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

class Funtions():
    
    def __init__(self) -> None:
        pass
    def create_table(eth_bridge,bridge:str) -> None:
        '''
            CREATE TABLE
            - Tạo và tính toán ra các dạng dữ liệu dạng bảng
        '''
        eth_bridge = eth_bridge[eth_bridge['bridge']==bridge]
        data_ht = eth_bridge[eth_bridge['time']==eth_bridge['time'].max()].reset_index()
        create_qk = eth_bridge.set_index('time')
        create_qk= create_qk.between_time('6:00','8:00').reset_index()
        create_qk['time']= pd.to_datetime(create_qk['time']).dt.date
        data_qk_24 = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(1)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        data = pd.concat([data_ht,data_qk_24],axis=1).drop(columns={'index'})
        data['change_1D'] = round(data['value'] - data['qk_vl'],2)
        data['pr_1D'] = round((data['value'] -data['qk_vl'])/data['value']*100,2)
        data_qk7d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(7)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk7d['qk_vl'].empty :
            data7D = pd.DataFrame({
                'change_7D':['comming soon'],
                'pr_7D':['comming soon']
            })
        else:
            data7D = pd.concat([data_ht,data_qk7d],axis=1).drop(columns={'index'})
            data7D['change_7D'] = round(data7D['value'] - data7D['qk_vl'],2)
            data7D['pr_7D'] = round((data7D['value'] -data7D['qk_vl'])/data7D['value']*100,2)

        data_qk30d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(30)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk30d['qk_vl'].empty :
            data30D = pd.DataFrame({
                'change_30D':['comming soon'],
                'pr_30D':['comming soon']
            })
        else:
            data30D = pd.concat([data_ht,data_qk30d],axis=1).drop(columns={'index'})
            data30D['change_30D'] = round(data30D['value'] - data30D['qk_vl'],2)
            data30D['pr_30D'] = round((data30D['value'] -data30D['qk_vl'])/data30D['value']*100,2)

        df_table = pd.concat([data,data7D,data30D],axis=1).drop(columns={'time'}).rename(columns={'value':'balance'})
        cols = ['bridge','balance','change_1D','pr_1D','change_7D','pr_7D','change_30D','pr_30D']
        df_table = df_table[cols]
        df_table['balance']= round(df_table['balance'],2)
        return df_table.to_dict(orient='records')
    def func_netflow(data,bridge:str,start:str,end:str) -> None:
        data = data[data['bridge']==bridge]
        data['qk_value'] = data['value'].shift(1).fillna(0)
        data = data.iloc[2:]
        data['change'] = round(data['value'] - data['qk_value'],2)
        data['money'] = round(data['change']* data['price'],2)
        data['time_select'] = pd.to_datetime(data['time']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols =['time','bridge','change','money','price']
        data = data[cols].rename(columns ={'time':'timestamp','bridge':'label'})
        return data.to_dict(orient='records')
    def create_bridge(eth_bridge,bridge:str,start:str,end:str)-> None:
        data = eth_bridge[eth_bridge['bridge']==bridge]
        data['time_select'] = pd.to_datetime(data['time']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols = ['time','value']
        data = data[cols].rename(columns={'time':'timestamp'})
        return data.to_dict(orient='records')
    
    
@eth_bridge_router.get('/pie')
async def pie():
    df_pie = eth_bridge.loc[eth_bridge['time'] == eth_bridge['time'].max()][['value','bridge']]
    return df_pie.to_dict(orient='records')

@eth_bridge_router.get('/balance')
async def bridge_ETH(bridge:str,start:str,end:str):
    choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base']
    if bridge not in choice_condition:
        return f'balance: {bridge} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base"]'
    elif bridge=="Arbitrum":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="Optimism":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="zkSync Era":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="StarkNet":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="Polygon":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="Linea":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="Base":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)

# NEtflow api in here
@eth_bridge_router.get('/Netflow')
async def func_netflow(bridge:str,start:str,end:str) -> None:
        choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base']
        if bridge not in choice_condition:
                return f'balance: {bridge} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base"]'
        elif bridge=="Arbitrum":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data
        elif bridge=="Optimism":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data
        elif bridge=="zkSync Era":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data
        elif bridge=="StarkNet":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data
        
        elif bridge=="Polygon":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data
        elif bridge=="Linea":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data
        elif bridge=="Base":
                data =Funtions.func_netflow(eth_bridge,bridge,start,end)
                return data

# API table
@eth_bridge_router.get('/table')
async def create_table(bridge:str):
    choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base']
    if bridge not in choice_condition:
            return f'balance: {bridge} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base"]'
    elif bridge=="Arbitrum":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    elif bridge=="Optimism":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    elif bridge=="zkSync Era":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    elif bridge=="StarkNet":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    elif bridge=="Polygon":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    elif bridge=="Linea":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    elif bridge=="Base":
          data = Funtions.create_table(eth_bridge,bridge)
          return data
    


