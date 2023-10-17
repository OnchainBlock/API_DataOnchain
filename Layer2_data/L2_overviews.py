import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
query_l2 = os.environ['query_l2']
server = os.environ['my_server']
query_tvl = os.environ['query_ETH_bridge']

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

TVL_df = pd.read_sql(query_tvl,server)

# config format time
TVL_df['time'] = TVL_df['time'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
#treemap
def treemap(condition:str):
    l2_condition = ['bridger','amount','fee','tx']
    data = L2_eth.groupby(['chain']).agg({'eth_amount':'sum','unique_users':'sum','fee_tx':'sum','tx':'sum'}).sort_values(by=['tx'],ascending=False).reset_index()
    if condition not in l2_condition:
        return f'layer2: {condition} is not found, plase choice another["bridger","amount","fee","tx"]'
    elif condition =='bridger':
        size =[500,250,150,100,60,40,32,30,26,10]
        data['size'] = [i for i in size[:len(data)]]
        return data[['chain','unique_users','size']].to_dict(orient="records")
    elif condition =='amount':
        size =[500,250,150,100,60,40,32,30,26,10]
        data['size'] = [i for i in size[:len(data)]]
        return data[['chain','eth_amount','size']].to_dict(orient="records")
    elif condition =='fee':
        size =[500,250,150,100,60,40,32,30,26,10]
        data['size'] = [i for i in size[:len(data)]]
        return data[['chain','fee_tx','size']].to_dict(orient="records")
    elif condition =='tx':
        size =[500,250,150,100,60,40,32,30,26,10]
        data['size'] = [i for i in size[:len(data)]]
        return data[['chain','tx','size']].to_dict(orient="records")

# overview table
def create_table_overview():
    table = L2_eth[L2_eth['dt'] ==  L2_eth['dt'].max()].sort_values(by=['chain'],ascending=False)[['eth_amount','fee_tx','tx','chain']]
    Qk_table =L2_eth[L2_eth['dt'] ==  L2_eth['dt'].max() - datetime.timedelta(days=1)].sort_values(by=['chain'],ascending=False)[['eth_amount','fee_tx','tx','chain']]
    volume_change =[vl - ql for vl,ql in zip(table['eth_amount'],Qk_table['eth_amount'])]
    fee_change = [vl - ql for vl,ql in zip(table['fee_tx'],Qk_table['fee_tx'])]
    tx_change = [vl - ql for vl,ql in zip(table['tx'],Qk_table['tx'])]
    tvl =TVL_df[TVL_df['time']==TVL_df['time'].max()].sort_values(by=['bridge'],ascending=False)[['value']]
    data = pd.DataFrame({
        'chain':[i for i in table['chain']],
        'tvl':[i for i in tvl['value']],
        'vl_change':volume_change,
        'fee_change':fee_change,
        'tx_change':tx_change,
        'per_vl':[round((qk/vl)*100,2) for qk,vl in zip(volume_change,table['eth_amount'])],
        'per_fee':[round((qk/vl)*100,2) for qk,vl in zip(fee_change,table['fee_tx'])],
        'per_tx':[round((qk/vl)*100,2) for qk,vl in zip(tx_change,table['tx'])]
    })
    return data.sort_values(by=['tvl'],ascending=False).to_dict(orient="records")


def create_statics_L2(condition:str):
    l2_condition = ['bridger','amount','fee','tx']
    data = L2_eth[L2_eth['dt']== L2_eth['dt'].max()]
    if condition not in l2_condition:
        return f'layer2: {condition} is not found, plase choice another["bridger","amount","fee","tx"]'
    elif condition =="bridger":
        return data[['chain','unique_users']].to_dict(orient='records')
    elif condition =="amount":
        return data[['chain','eth_amount']].to_dict(orient='records')
    elif condition =="fee":
        return data[['chain','fee_tx']].to_dict(orient='records')
    elif condition =="tx":
        return data[['chain','tx']].to_dict(orient='records')

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
