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

class Funtions():
    
    def __init__(self) -> None:
        pass
    
    def create_table(eth_bridge)->None:
        data_ht = eth_bridge[eth_bridge['time']==eth_bridge['time'].max()].reset_index()
        create_qk = eth_bridge.set_index('time')
        create_qk= create_qk.between_time('6:00','9:00').reset_index()
        create_qk['time']= pd.to_datetime(create_qk['time']).dt.date
        data_qk_24 = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(1)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        data = pd.concat([data_ht,data_qk_24],axis=1).drop(columns={'index'})
        data['cvl_1D'] = round(data['value'] - data['qk_vl'],2)
        data['pr_1D'] = round((data['value'] -data['qk_vl'])/data['value']*100,2)
        data_qk7d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(7)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk7d['qk_vl'].empty :
                    data7D = pd.DataFrame({
                        'cvl_7D':['-'],
                        'pr_7D':['-']
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
                'cvl_30D':['-'],
                'pr_30D':['-']
            })
        else:
            data30D = pd.concat([data_ht,data_qk30d],axis=1).drop(columns={'index'})
            data30D['cvl_30D'] = round(data30D['value'] - data30D['qk_vl'],2)
            data30D['pr_30D'] = round((data30D['value'] -data30D['qk_vl'])/data30D['value']*100,2)

        df_table = pd.concat([data,data7D,data30D],axis=1).drop(columns={'time'}).rename(columns={'value':'balance'})
        cols_main = ['bridge','balance','cvl_1D', 'pr_1D','cvl_7D', 'pr_7D','cvl_30D', 'pr_30D']
        df_table = df_table[cols_main]
        df_table = df_table.fillna('-')
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
        Scroll = Funtions.func_netflow(eth_bridge,'Scroll')
        data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta,Scroll]
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
        data_ht = eth_bridge[eth_bridge['time']==eth_bridge['time'].max()].reset_index()
        create_qk = eth_bridge.set_index('time')
        create_qk= create_qk.between_time('6:00','9:00').reset_index()
        create_qk['time']= pd.to_datetime(create_qk['time']).dt.date
        data_qk_24 = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(1)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        data = pd.concat([data_ht,data_qk_24],axis=1).drop(columns={'index'})
        data['cvl_1D'] = round(data['value'] - data['qk_vl'],2)
        data['pr_1D'] = round((data['value'] -data['qk_vl'])/data['value']*100,2)
        data_qk7d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(7)][['value']].rename(columns={'value':'qk_vl'}).reset_index()
        if data_qk7d['qk_vl'].empty :
                    data7D = pd.DataFrame({
                        'cvl_7D':['-'],
                        'pr_7D':['-']
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
                'cvl_30D':['-'],
                'pr_30D':['-']
            })
        else:
            data30D = pd.concat([data_ht,data_qk30d],axis=1).drop(columns={'index'})
            data30D['cvl_30D'] = round(data30D['value'] - data30D['qk_vl'],2)
            data30D['pr_30D'] = round((data30D['value'] -data30D['qk_vl'])/data30D['value']*100,2)

        df_table = pd.concat([data,data7D,data30D],axis=1).drop(columns={'time'}).rename(columns={'value':'balance'})
        cols_main = ['bridge','balance','cvl_1D', 'pr_1D','cvl_7D', 'pr_7D','cvl_30D', 'pr_30D']
        df_table = df_table[cols_main]
        df_table = df_table.fillna('-')
        df_table['balance']= round(df_table['balance'],2)
        return df_table.to_dict(orient="records")
    
#create Inflow / OutFlow Ethereum in layers2

@eth_bridge_router.get('/Inflow_layer2')
async def Inflow_layer2(start:str,end:str,label:str):
     return Funtions_TVL.Inflow_layer2(eth_bridge,start,end,label)
        
    
@eth_bridge_router.get('/Outflow_layer2')
async def Outflow_layer2(start:str,end:str,label:str):
    return Funtions_TVL.OutFlow(eth_bridge,start,end,label)