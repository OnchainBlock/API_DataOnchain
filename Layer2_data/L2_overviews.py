import sys
sys.path.append(r'/root/API_DataOnchain')
from imports import *
query_l2 = os.environ['query_l2']
server = os.environ['my_server']

L2_eth = pd.read_sql(query_l2,server)
condition_time = datetime.datetime.now().strftime('%Y-%m-%d')
L2_eth['dt'] =  L2_eth['dt'].map(lambda x : x.replace(' UTC',''))
L2_eth['dt'] = pd.to_datetime(L2_eth['dt']).dt.date

#weekly
weekly_df = L2_eth.copy()
weekly_df['dt']= pd.to_datetime(weekly_df['dt'])
weekly_df =weekly_df.groupby(['chain']).resample('W', on='dt')[['eth_amount', 'unique_users', 'fee_tx',
                                           'volume_bridge', 'fee_volume', 'tx', 'chain']].sum(numeric_only=True).sort_values(by=['dt'],ascending=True).reset_index()

weekly_df =weekly_df[weekly_df['dt']<= condition_time]
#treemap
def treemap():
    data = L2_eth[L2_eth['dt']== L2_eth['dt'].max()].sort_values(by=['eth_amount'],ascending=False)[['chain','eth_amount']]
    size =[500,250,150,100,60,40,32,30,26]
    data['size'] = [i for i in size[:len(data)]]
    return data.to_dict(orient="records")

# overview table
def create_table_overview():
    table = L2_eth[L2_eth['dt'] ==  L2_eth['dt'].max()].sort_values(by=['chain'],ascending=False)[['eth_amount','fee_tx','tx','chain']]
    Qk_table =L2_eth[L2_eth['dt'] ==  L2_eth['dt'].max() - datetime.timedelta(days=1)].sort_values(by=['chain'],ascending=False)[['eth_amount','fee_tx','tx','chain']]
    volume_change =[vl - ql for vl,ql in zip(table['eth_amount'],Qk_table['eth_amount'])]
    fee_change = [vl - ql for vl,ql in zip(table['fee_tx'],Qk_table['fee_tx'])]
    tx_change = [vl - ql for vl,ql in zip(table['tx'],Qk_table['tx'])]

    data = pd.DataFrame({
        'chain':[i for i in table['chain']],
        'vl_change':volume_change,
        'fee_change':fee_change,
        'tx_change':tx_change,
        'per_vl':[round((qk/vl)*100,2) for qk,vl in zip(volume_change,table['eth_amount'])],
        'per_fee':[round((qk/vl)*100,2) for qk,vl in zip(fee_change,table['fee_tx'])],
        'per_tx':[round((qk/vl)*100,2) for qk,vl in zip(tx_change,table['tx'])]
    })
    return data.sort_values(by=['vl_change'],ascending=False).to_dict(orient="records")

#create statics
def create_statics_L2(l2:str):
    l2_condition = ['starknet', 'arbitrum', 'polygon', 'optimsn', 'zk_era', 'base',
       'mantle', 'linear', 'manta','scroll']
    
    if l2 not in l2_condition:
        return f'layer2: {l2} is not found, plase choice another["starknet", "arbitrum", "polygon", "optimsn", "zk_era", "base","mantle", "linear", "manta","scroll"]'
    else:
        data = L2_eth[L2_eth['chain']==l2]
    return pd.DataFrame({
        'Total_User':[sum(data['unique_users'])],
        'Total_ETH':[sum(data['eth_amount'])],
        'Total_Tx':[sum(data['tx'])],
        'Total_Fee':[sum(data['fee_tx'])]
    }).to_dict(orient="records")

# create dataFrame for 
class Func_Layer2():
    def __init__(self) -> None:
        pass
    def Daily(l2:str,start:str,end:str,col_condition:str):
        l2_condition = ['starknet', 'arbitrum', 'polygon', 'optimsn', 'zk_era', 'base',
       'mantle', 'linear', 'manta','scroll']
    
        if l2 not in l2_condition:
            return f'layer2: {l2} is not found, plase choice another["starknet", "arbitrum", "polygon", "optimsn", "zk_era", "base","mantle", "linear", "manta","scroll"]'
        data = L2_eth[L2_eth['chain']==l2]
        data['time_select'] = pd.to_datetime(data['dt']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        if col_condition == 'unique_users':
            cols=['dt','chain','unique_users']
            return data[cols].to_dict(orient="records")
        elif col_condition=='eth_amount':
            cols =['dt','chain','eth_amount']
            return data[cols].to_dict(orient="records")
        elif col_condition=='tx':
            cols =['dt','chain','tx']
            return data[cols].to_dict(orient="records")
        elif col_condition=='fee_tx':
            cols =['dt','chain','fee_tx']
            return data[cols].to_dict(orient="records")
    def Weekly(l2:str,start:str,end:str,col_condition:str):
        l2_condition = ['starknet', 'arbitrum', 'polygon', 'optimsn', 'zk_era', 'base',
       'mantle', 'linear', 'manta','scroll']
    
        if l2 not in l2_condition:
            return f'layer2: {l2} is not found, plase choice another["starknet", "arbitrum", "polygon", "optimsn", "zk_era", "base","mantle", "linear", "manta","scroll"]'
        data = weekly_df[weekly_df['chain']==l2]
        data['time_select'] = pd.to_datetime(data['dt']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        if col_condition == 'unique_users':
            cols=['dt','chain','unique_users']
            return data[cols].to_dict(orient="records")
        elif col_condition=='eth_amount':
            cols =['dt','chain','eth_amount']
            return data[cols].to_dict(orient="records")
        elif col_condition=='tx':
            cols =['dt','chain','tx']
            return data[cols].to_dict(orient="records")
        elif col_condition=='fee_tx':
            cols =['dt','chain','fee_tx']
            return data[cols].to_dict(orient="records")
