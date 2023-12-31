import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *

server = os.environ['my_server']
query_tvl = os.environ['query_ETH_bridge']
query_l2_tx = os.environ['query_l2']

L2_eth = pd.read_sql(query_l2_tx,server)
L2_eth['dt'] =  L2_eth['dt'].map(lambda x : x.replace(' UTC',''))
L2_eth['dt'] = pd.to_datetime(L2_eth['dt']).dt.date
condition_time = datetime.datetime.now().strftime('%Y-%m-%d')
#weekly
weekly_df = L2_eth.copy()
weekly_df['dt']= pd.to_datetime(weekly_df['dt'])
weekly_df =weekly_df.groupby(['chain']).resample('W', on='dt')[['eth_amount', 'unique_users', 'fee_tx',
                                           'volume_bridge', 'fee_volume', 'tx', 'chain']].sum(numeric_only=True).sort_values(by=['dt'],ascending=True).reset_index()

weekly_df =weekly_df[weekly_df['dt']<= condition_time]

# Total value unlock Layer2
TVL_df = pd.read_sql(query_tvl,server)

# config format time
TVL_df['time'] = TVL_df['time'].apply(
    lambda x: pd.to_datetime(x).floor('T'))

def sort_label():
    return TVL_df[TVL_df['time']== TVL_df['time'].max()].sort_values(by=['value'],ascending=False)[['bridge']]
def tx_layer2_time(time:str,l2:str,start:str,end:str):
    '''
    content: chart transaction with have unique user, amount eth, fee
    '''
    choice_condition = ['daily','weekly']
    if time not in choice_condition:
        return f'balance: {time} is not found, plase choice another ["daily","weekly"]'
    elif time =='daily':
        data = L2_eth[L2_eth['chain']==l2][['dt','eth_amount','unique_users','fee_tx']]
        data['dt'] = pd.to_datetime(data['dt'])
        data = data[data['dt'].between(start,end)]
        return data.rename(columns={'dt':'timestamp'}).to_dict(orient="records")
    elif time =='weekly':
        data = weekly_df[weekly_df['chain']==l2][['dt','eth_amount','unique_users','fee_tx']]
        data['dt'] = pd.to_datetime(data['dt'])
        data = data[data['dt'].between(start,end)]
        return data.rename(columns={'dt':'timestamp'}).to_dict(orient="records")

def create_overview_Layer2(l2_tvl:str,l2_tx:str):
    tvl =TVL_df[TVL_df['time']==TVL_df['time'].max()]
    tvl_value=tvl[tvl['bridge']==l2_tvl]['value'].values
    l2_tracking = L2_eth[L2_eth['chain']==l2_tx]
    return pd.DataFrame({
        'Tvl':tvl_value,
        'Tvl_money': tvl_value* tvl['price'].unique(),
        'Total_User':[sum(l2_tracking['unique_users'])],
        'Total_ETH':[sum(l2_tracking['eth_amount'])],
        'Total_Tx':[sum(l2_tracking['tx'])],
        'Total_Fee':[sum(l2_tracking['fee_tx'])]
    }).to_dict(orient='records')
    
#statis 1D, 7D, 1M
class Funtions_TVL():
    '''
    # funtion include statistic 1D,7D,1M , inflow, outflow, tvl
    '''
    
    def __init__(self) -> None:
        pass
    
    def create_table(l2:str)->None:
        
        data_ht = TVL_df[TVL_df['time']==TVL_df['time'].max()].sort_values(by=['bridge'],ascending=False).reset_index()
        data_ht_condition = data_ht.copy()
        create_qk = TVL_df.set_index('time')
        create_qk= create_qk.between_time('6:00','9:00').reset_index()
        create_qk['time']= pd.to_datetime(create_qk['time']).dt.date
        data_qk_24 = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(1)].rename(columns={'value':'qk_vl'}).sort_values(by=['bridge'],ascending=False).reset_index()[['qk_vl']]
        data = pd.concat([data_ht,data_qk_24],axis=1)
        data['cvl_1D'] = round(data['value'] - data['qk_vl'],2)
        data['pr_1D'] = round((data['value'] -data['qk_vl'])/data['value']*100,2)
        data_qk7d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(7)].rename(columns={'value':'qk_vl'}).sort_values(by=['bridge'],ascending=False).reset_index()[['qk_vl']]
        data_qk30d = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(30)].rename(columns={'value':'qk_vl'}).sort_values(by=['bridge'],ascending=False).reset_index()[['qk_vl']]
        condition_name = create_qk[create_qk['time'] == create_qk['time'].max()- datetime.timedelta(30)]['bridge'].unique()
        data_ht_condition = data_ht_condition[data_ht_condition['bridge'].isin(condition_name)].sort_values(by=['bridge'],ascending=False).reset_index()
        if data_qk7d['qk_vl'].empty :
                    data7D = pd.DataFrame({
                        'cvl_7D':['-'],
                        'pr_7D':['-']
                    })
        else:
            data7D = pd.concat([data_ht,data_qk7d],axis=1)
            data7D['cvl_7D'] = round(data7D['value'] - data7D['qk_vl'],2)
            data7D['pr_7D'] = round((data7D['value'] -data7D['qk_vl'])/data7D['value']*100,2)
            cols_7d = ['cvl_7D','pr_7D']
            data7D = data7D[cols_7d]

        
        if data_qk30d['qk_vl'].empty :
            data30D = pd.DataFrame({
                'cvl_30D':['-'],
                'pr_30D':['-']
            })
        else:
            data30D = pd.concat([data_ht_condition,data_qk30d],axis=1)
            data30D['cvl_30D'] = round(data30D['value'] - data30D['qk_vl'],2)
            data30D['pr_30D'] = round((data30D['value'] -data30D['qk_vl'])/data30D['value']*100,2)

        df_table = pd.concat([data,data7D,data30D],axis=1).drop(columns={'time'}).rename(columns={'value':'balance'})
        cols_main = ['bridge','balance','cvl_1D', 'pr_1D','cvl_7D', 'pr_7D','cvl_30D', 'pr_30D']
        df_table = df_table[cols_main]
        df_table = df_table.loc[:, ~df_table.columns.duplicated()]
        df_table = df_table.fillna('-')
        df_table = df_table.sort_values(by=['balance'],ascending=False)
        if l2 =="None":
             
            return df_table.to_dict(orient="records")
        else:
            df_table = df_table[df_table['bridge']==l2]
            return df_table.to_dict(orient="records")
        

    def func_netflow(data,bridge:str) -> None:
        data = data[data['bridge']==bridge]
        data['qk_value'] = data['value'].shift(1).fillna(0)
        data = data.iloc[2:]
        
        data['change'] = round(data['value'] - data['qk_value'],2)
        
        data = data.rename(columns ={'time':'timestamp','bridge':'label'})
        return data
    def create_netflow_df(eth_bridge,start:str,end:str):
        Arbitrum = Funtions_TVL.func_netflow(eth_bridge,'Arbitrum')
        Optimism = Funtions_TVL.func_netflow(eth_bridge,'Optimism')
        zkSync_Era = Funtions_TVL.func_netflow(eth_bridge,'zkSync Era')
        StarkNet = Funtions_TVL.func_netflow(eth_bridge,'StarkNet')
        Polygon = Funtions_TVL.func_netflow(eth_bridge,'Polygon ZKEVM')
        Linea = Funtions_TVL.func_netflow(eth_bridge,'Linea')
        Base = Funtions_TVL.func_netflow(eth_bridge,'Base')
        Mantle = Funtions_TVL.func_netflow(eth_bridge,'Mantle')
        Manta = Funtions_TVL.func_netflow(eth_bridge,'Manta')
        Scroll = Funtions_TVL.func_netflow(eth_bridge,'Scroll')
        data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta,Scroll]
        data = pd.concat(data,axis=0)
        data['time_select'] = pd.to_datetime(data['timestamp']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols = ['timestamp','label','change']
        data = data[cols].rename(columns={'change':'value'})
        
        # data = data[cols]
        return data.to_dict(orient='records')
        
       
    def Inflow_layer2(eth_bridge,start:str,end:str,label:str):
     
        choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon ZKEVM','Linea', 'Base', 'Mantle','Manta','Scroll']
        if label not in choice_condition:
            return f'balance: {label} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon ZKEVM","Linea", "Base", "Mantle","Manta","Scroll"]'
        
        else:

            Arbitrum = Funtions_TVL.func_netflow(eth_bridge,'Arbitrum')
            Optimism = Funtions_TVL.func_netflow(eth_bridge,'Optimism')
            zkSync_Era = Funtions_TVL.func_netflow(eth_bridge,'zkSync Era')
            StarkNet = Funtions_TVL.func_netflow(eth_bridge,'StarkNet')
            Polygon = Funtions_TVL.func_netflow(eth_bridge,'Polygon ZKEVM')
            Linea = Funtions_TVL.func_netflow(eth_bridge,'Linea')
            Base = Funtions_TVL.func_netflow(eth_bridge,'Base')
            Mantle = Funtions_TVL.func_netflow(eth_bridge,'Mantle')
            Manta = Funtions_TVL.func_netflow(eth_bridge,'Manta')
            Scroll = Funtions_TVL.func_netflow(eth_bridge,'Scroll')
            data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta,Scroll]
            data = pd.concat(data,axis=0)
            data = data[data['label']==label]
            data = data[data['change']>0]
            data = data.sort_values(by=['timestamp'],ascending=True)
            data['time_select'] = pd.to_datetime(data['timestamp']).dt.date
            data['time_select'] = pd.to_datetime(data['time_select'])
            data = data[data['time_select'].between(start,end)]
            cols = ['timestamp','label','change']
            data = data[cols].rename(columns={'change':'value'})
            return data.to_dict(orient="records")
    def OutFlow(eth_bridge,start:str,end:str,label:str):
        choice_condition = ['Arbitrum', 'Optimism', 'zkSync Era', 'StarkNet', 'Polygon ZKEVM','Linea', 'Base', 'Mantle','Manta','Scroll']
        if label not in choice_condition:
            return f'balance: {label} is not found, plase choice another ["Arbitrum", "Optimism", "zkSync Era", "StarkNet", "Polygon ZKEVM","Linea", "Base", "Mantle","Manta","Scroll"]'
        
        else:

            Arbitrum = Funtions_TVL.func_netflow(eth_bridge,'Arbitrum')
            Optimism = Funtions_TVL.func_netflow(eth_bridge,'Optimism')
            zkSync_Era = Funtions_TVL.func_netflow(eth_bridge,'zkSync Era')
            StarkNet = Funtions_TVL.func_netflow(eth_bridge,'StarkNet')
            Polygon = Funtions_TVL.func_netflow(eth_bridge,'Polygon ZKEVM')
            Linea = Funtions_TVL.func_netflow(eth_bridge,'Linea')
            Base = Funtions_TVL.func_netflow(eth_bridge,'Base')
            Mantle = Funtions_TVL.func_netflow(eth_bridge,'Mantle')
            Manta = Funtions_TVL.func_netflow(eth_bridge,'Manta')
            Scroll = Funtions_TVL.func_netflow(eth_bridge,'Scroll')
            data = [Arbitrum,Optimism,zkSync_Era,StarkNet,Polygon,Linea,Base,Mantle,Manta,Scroll]
            data = pd.concat(data,axis=0)
            data = data[data['label']==label]
            data = data[data['change']<0]
            data = data.sort_values(by=['timestamp'],ascending=True)
            data['time_select'] = pd.to_datetime(data['timestamp']).dt.date
            data['time_select'] = pd.to_datetime(data['time_select'])
            data = data[data['time_select'].between(start,end)]
            cols = ['timestamp','label','change']
            data = data[cols].rename(columns={'change':'value'})
            return data.to_dict(orient="records")
    
    def create_bridge(bridge:str,start:str,end:str)-> None:
        data = TVL_df[TVL_df['bridge']==bridge]
        data['time_select'] = pd.to_datetime(data['time']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols = ['time','value']
        data = data[cols].rename(columns={'time':'timestamp'})
        return data.to_dict(orient='records')
    

