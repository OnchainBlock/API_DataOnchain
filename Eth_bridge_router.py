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
    
    def create_table(eth_bridge)->None:
        data_ht = eth_bridge[eth_bridge['time']==eth_bridge['time'].max()].reset_index()
        create_qk = eth_bridge.set_index('time')
        create_qk= create_qk.between_time('6:00','8:00').reset_index()
        create_qk['time']= pd.to_datetime(create_qk['time']).dt.date
        data_qk_24 = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(1)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        data = pd.concat([data_ht,data_qk_24],axis=1).drop(columns={'index'})
        data['cvl_1D'] = round(data['value'] - data['qk_vl'],2)
        data['pr_1D'] = round((data['value'] -data['qk_vl'])/data['value']*100,2)
        data_qk7d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(7)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk7d['qk_vl'].empty :
                    data7D = pd.DataFrame({
                        'cvl_7D':['comming soon'],
                        'pr_7D':['comming soon']
                    })
        else:
            data7D = pd.concat([data_ht,data_qk7d],axis=1).drop(columns={'index'})
            data7D['cvl_7D'] = round(data7D['value'] - data7D['qk_vl'],2)
            data7D['pr_7D'] = round((data7D['value'] -data7D['qk_vl'])/data7D['value']*100,2)
            cols_7d = ['cvl_7D','pr_7D']
            data7D = data7D[cols_7d]

        data_qk30d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(30)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk30d['qk_vl'].empty :
            data30D = pd.DataFrame({
                'cvl_30D':['comming soon'],
                'pr_30D':['comming soon']
            })
        else:
            data30D = pd.concat([data_ht,data_qk30d],axis=1).drop(columns={'index'})
            data30D['cvl_30D'] = round(data30D['value'] - data30D['qk_vl'],2)
            data30D['pr_30D'] = round((data30D['value'] -data30D['qk_vl'])/data30D['value']*100,2)

        df_table = pd.concat([data,data7D,data30D],axis=1).drop(columns={'time'}).rename(columns={'value':'balance'})
        cols_main = ['bridge','balance','cvl_1D', 'pr_1D','cvl_7D', 'pr_7D','cvl_30D', 'pr_30D']
        df_table = df_table[cols_main]
        df_table = df_table.fillna('comming soon')
        df_table['balance']= round(df_table['balance'],2)
        return df_table.to_dict(orient="records")
    def func_netflow(data,bridge:str) -> None:
        data = data[data['bridge']==bridge]
        data['qk_value'] = data['value'].shift(1).fillna(0)
        data = data.iloc[2:]
        
        data['change'] = round(data['value'] - data['qk_value'],2)
        
        data = data.rename(columns ={'time':'timestamp','bridge':'label'})
        return data
    def create_netflow_df(eth_bridge,start:str,end:str):
        Arbitrum = Funtions.func_netflow(eth_bridge,'Arbitrum')
        Optimism = Funtions.func_netflow(eth_bridge,'Optimism')
        zkSync_Era = Funtions.func_netflow(eth_bridge,'zkSync Era')
        StarkNet = Funtions.func_netflow(eth_bridge,'StarkNet')
        Polygon = Funtions.func_netflow(eth_bridge,'Polygon')
        Linea = Funtions.func_netflow(eth_bridge,'Linea')
        Base = Funtions.func_netflow(eth_bridge,'Base')
        Mantle = Funtions.func_netflow(eth_bridge,'Mantle')
        Manta = Funtions.func_netflow(eth_bridge,'Manta')

        data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta]
        data = pd.concat(data,axis=0)
        data['time_select'] = pd.to_datetime(data['timestamp']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols = ['timestamp','label','change']
        data = data[cols].rename(columns={'change':'value'}).groupby(['timestamp','label','value']).count().reset_index()
        
        # data = data[cols]
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
    df_pie = eth_bridge.loc[eth_bridge['time'] == eth_bridge['time'].max()][['value','bridge']].rename(columns={'value':'VALUE','bridge':'EXPLORER'})
    df_pie = df_pie.sort_values(by=['VALUE'],ascending=False)
    return df_pie.to_dict(orient='records')

@eth_bridge_router.get('/balance')
async def bridge_ETH(bridge:str,start:str,end:str):
    choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base','Mantle','Manta']
    if bridge not in choice_condition:
        return f'balance: {bridge} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base","Mantle","Manta"]'
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
    elif bridge=="Mantle":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)
    elif bridge=="Manta":
        return Funtions.create_bridge(eth_bridge,bridge,start,end)

# NEtflow api in here
@eth_bridge_router.get('/Netflow')
async def func_netflow(start:str,end:str) -> None:
        return Funtions.create_netflow_df(eth_bridge,start,end)
        

# API table
@eth_bridge_router.get('/table')
async def create_table()->None:
        data_ht = eth_bridge[eth_bridge['time']==eth_bridge['time'].max()].reset_index()
        create_qk = eth_bridge.set_index('time')
        create_qk= create_qk.between_time('6:00','8:00').reset_index()
        create_qk['time']= pd.to_datetime(create_qk['time']).dt.date
        data_qk_24 = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(1)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        data = pd.concat([data_ht,data_qk_24],axis=1).drop(columns={'index'})
        data['cvl_1D'] = round(data['value'] - data['qk_vl'],2)
        data['pr_1D'] = round((data['value'] -data['qk_vl'])/data['value']*100,2)
        data_qk7d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(7)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk7d['qk_vl'].empty :
                    data7D = pd.DataFrame({
                        'cvl_7D':['comming soon'],
                        'pr_7D':['comming soon']
                    })
        else:
            data7D = pd.concat([data_ht,data_qk7d],axis=1).drop(columns={'index'})
            data7D['cvl_7D'] = round(data7D['value'] - data7D['qk_vl'],2)
            data7D['pr_7D'] = round((data7D['value'] -data7D['qk_vl'])/data7D['value']*100,2)
            cols_7d = ['cvl_7D','pr_7D']
            data7D = data7D[cols_7d]

        data_qk30d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(30)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk30d['qk_vl'].empty :
            data30D = pd.DataFrame({
                'cvl_30D':['comming soon'],
                'pr_30D':['comming soon']
            })
        else:
            data30D = pd.concat([data_ht,data_qk30d],axis=1).drop(columns={'index'})
            data30D['cvl_30D'] = round(data30D['value'] - data30D['qk_vl'],2)
            data30D['pr_30D'] = round((data30D['value'] -data30D['qk_vl'])/data30D['value']*100,2)

        df_table = pd.concat([data,data7D,data30D],axis=1).drop(columns={'time'}).rename(columns={'value':'balance'})
        cols_main = ['bridge','balance','cvl_1D', 'pr_1D','cvl_7D', 'pr_7D','cvl_30D', 'pr_30D']
        df_table = df_table[cols_main]
        df_table = df_table.fillna('comming soon')
        df_table['balance']= round(df_table['balance'],2)
        return df_table.to_dict(orient="records")
    
#create Inflow / OutFlow Ethereum in layers2
def func_netflow(data,bridge:str) -> None:
        data = data[data['bridge']==bridge]
        data['qk_value'] = data['value'].shift(1).fillna(0)
        data = data.iloc[2:]
        
        data['change'] = round(data['value'] - data['qk_value'],2)
        data['money'] = round(data['change']* data['price'],2)
        cols =['time','bridge','change','money']
        data = data[cols].rename(columns ={'time':'timestamp','bridge':'label','change':'value'})
        return data

@eth_bridge_router.get('/Inflow_layer2')
async def Inflow_layer2(start:str,end:str,label:str):
        choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base', 'Mantle','Manta']
        if label not in choice_condition:
                return f'balance: {label} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base", "Mantle","Manta"]'
        
        else:

                Arbitrum = func_netflow(eth_bridge,'Arbitrum')
                Optimism = func_netflow(eth_bridge,'Optimism')
                zkSync_Era = func_netflow(eth_bridge,'zkSync Era')
                StarkNet = func_netflow(eth_bridge,'StarkNet')
                Polygon = func_netflow(eth_bridge,'Polygon')
                Linea = func_netflow(eth_bridge,'Linea')
                Base = func_netflow(eth_bridge,'Base')
                Mantle = func_netflow(eth_bridge,'Mantle')
                Manta = func_netflow(eth_bridge,'Manta')
                data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta]
                data = pd.concat(data,axis=0)
                data = data[data['label']==label]
                data = data[data['value']>0]
                data = data.sort_values(by=['timestamp'],ascending=True)
                data['time_select'] = pd.to_datetime(data['timestamp']).dt.date
                data['time_select'] = pd.to_datetime(data['time_select'])
                data = data[data['time_select'].between(start,end)]
                cols = ['timestamp','label','value','money']
                data = data[cols]
                return data.to_dict(orient="records")
    
@eth_bridge_router.get('/Outflow_layer2')
async def Outflow_layer2(start:str,end:str,label:str):
        choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base', 'Mantle']
        if label not in choice_condition:
                return f'balance: {label} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base", "Mantle"]'
        
        else:
            Arbitrum = func_netflow(eth_bridge,'Arbitrum')
            Optimism = func_netflow(eth_bridge,'Optimism')
            zkSync_Era = func_netflow(eth_bridge,'zkSync Era')
            StarkNet = func_netflow(eth_bridge,'StarkNet')
            Polygon = func_netflow(eth_bridge,'Polygon')
            Linea = func_netflow(eth_bridge,'Linea')
            Base = func_netflow(eth_bridge,'Base')
            Mantle = func_netflow(eth_bridge,'Mantle')
            Manta = func_netflow(eth_bridge,'Manta')
            data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta]
            data = pd.concat(data,axis=0)
            data = data[data['label']==label]
            data = data[data['value']<0]
            data = data.sort_values(by=['timestamp'],ascending=True)
            data['time_select'] = pd.to_datetime(data['timestamp']).dt.date
            data['time_select'] = pd.to_datetime(data['time_select'])
            data = data[data['time_select'].between(start,end)]
            cols = ['timestamp','label','value','money']
            data = data[cols]
            return data.to_dict(orient="records")
        
        
# api balance of layer2
      
def func_layer2(eth_bridge,layer2:str):
    data =eth_bridge[eth_bridge['time']== eth_bridge['time'].max()]
    data = data[data['bridge']==layer2]
    return pd.DataFrame({
        'balanceofeth':data['value'],
        'usd':data['value']*data['price']
    }).to_dict(orient='records')

@eth_bridge_router.get('/balanceofL2')
async def balanceofLayer2(layer2:str):
    choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon','Linea', 'Base','Mantle','Manta']
    if layer2 not in choice_condition:
        return f'balance: {layer2} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon","Linea", "Base","Mantle","Manta"]'
    elif layer2=="Arbitrum":
        return func_layer2(eth_bridge,'Arbitrum')
    elif layer2=="Optimism":
        return func_layer2(eth_bridge,'Optimism')
    elif layer2=="zkSync Era":
        return func_layer2(eth_bridge,'zkSync Era')
    elif layer2=="StarkNet":
        return func_layer2(eth_bridge,'StarkNet')
    elif layer2=="Polygon":
        return func_layer2(eth_bridge,'Polygon')
    elif layer2=="Linea":
        return func_layer2(eth_bridge,'Linea')
    elif layer2=="Base":
        return func_layer2(eth_bridge,'Base')
    elif layer2=="Mantle":
        return func_layer2(eth_bridge,'Mantle')
    elif layer2=="Manta":
        return func_layer2(eth_bridge,'Manta')