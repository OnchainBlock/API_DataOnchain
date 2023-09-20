from imports import *
import pandas as pd


eth_router = APIRouter(
    prefix='/overview',
    tags=['ETH'],

)

my_server = os.environ['my_server']
query_ETH = os.environ['query_ETH']

ETH_psql = pd.read_sql(query_ETH,my_server)

ETH_psql['time'] = ETH_psql['time'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
df_total_line =ETH_psql.groupby(['time','price'])[['value']].agg({'value':'sum'}).reset_index()

df_treemap = ETH_psql.copy()
df_treemap['time'] = df_treemap['time'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
QK_df_treemap = df_treemap.set_index('time')
QK_df_treemap = QK_df_treemap.between_time('6:00', '10:59')
QK_df_treemap = QK_df_treemap.reset_index()
QK_df_treemap['time'] = pd.to_datetime(QK_df_treemap['time']).dt.date

def capitalize_column(dataframe, column_name):
    dataframe[column_name] = dataframe[column_name].str.capitalize()
    return dataframe
def change_name(df,cols:str):
    df[cols] = df[cols].replace({'Crypto':'Crypto.com','Binance_us':'Binance US','Mexc':'MEXC','Okx':'OKX'})
    return df

ETH_psql = capitalize_column(ETH_psql,'balance')
ETH_psql = change_name(ETH_psql,'balance')
@eth_router.get('/eth/pie_eth')
async def choice():
    df_pie = ETH_psql.loc[ETH_psql['time'] == ETH_psql['time'].max()][['time','value','balance']]
    return df_pie.to_dict(orient='records')


@eth_router.get('/eth/treemap')
async def Treemap_ETH(choice_days:int):
    hientai_df =df_treemap[df_treemap['time'] == df_treemap['time'].max()][['value','balance']]
    quakhu_df =QK_df_treemap[QK_df_treemap['time'] == QK_df_treemap['time'].max() - datetime.timedelta(days=choice_days)][['value','balance']].rename(columns={'value':'vl_qk','balance':'balance_qk'})
    hientai_df = hientai_df.sort_values(by=['value'],ascending=False).reset_index()
    lst_blance = list(hientai_df['balance'].unique())
    quakhu_df = quakhu_df.set_index('balance_qk')
    quakhu_df =quakhu_df.loc[lst_blance].reset_index()
    eth_tmap = pd.concat([hientai_df,quakhu_df],axis=1)
    eth_tmap['vl_change'] = (eth_tmap['value'] - eth_tmap['vl_qk'])
    eth_tmap['percentage'] = round((eth_tmap['vl_change']/eth_tmap['value']) *100,2)
    size = [500,250,150,100,60,40,32,30,26,20,15,13,10,7,6,5,4,3,3,3]
    eth_tmap['size'] = [i for i in size[:len(eth_tmap)]]
    cols = ['balance','value','vl_change','percentage','size']
    eth_tmap= eth_tmap[cols].rename(columns={'balance':'Symbols','value':'VALUE','vl_change':'VL_CHANGE','percentage':'PERCENTAGE'})
    eth_tmap =eth_tmap.drop(eth_tmap[eth_tmap['PERCENTAGE']==0.00].index)
    eth_tmap = capitalize_column(eth_tmap,'Symbols')
    eth_tmap = change_name(eth_tmap,'Symbols')
    return eth_tmap.to_dict(orient='records')


@eth_router.get('/eth/top_netflow')
async def hightlight_ETH(choice_days:int,label:str):
    hientai_df =df_treemap[df_treemap['time'] == df_treemap['time'].max()][['value','balance','price']]
    quakhu_df =QK_df_treemap[QK_df_treemap['time'] == QK_df_treemap['time'].max() - datetime.timedelta(days=choice_days)][['value','balance']].rename(columns={'value':'vl_qk','balance':'balance_qk'})
    hientai_df = hientai_df.sort_values(by=['value'],ascending=False).reset_index()
    lst_blance = list(hientai_df['balance'].unique())
    quakhu_df = quakhu_df.set_index('balance_qk')
    quakhu_df =quakhu_df.loc[lst_blance].reset_index()
    eth_tmap = pd.concat([hientai_df,quakhu_df],axis=1)
    eth_tmap['VALUE_SHOW'] = eth_tmap['value'].map(lambda x : numerize.numerize(x))
    eth_tmap['vl_change'] = (eth_tmap['value'] - eth_tmap['vl_qk'])
    eth_tmap['percentage'] = round((eth_tmap['vl_change']/eth_tmap['value']) *100,2)
    cols = ['balance','value','VALUE_SHOW','percentage']
    eth_tmap= eth_tmap[cols].rename(columns={'balance':'Symbols','value':'VALUE','percentage':'PERCENTAGE'})
    eth_tmap =eth_tmap.drop(eth_tmap[eth_tmap['PERCENTAGE']==0.00].index)
    cols = ['Symbols','VALUE','VALUE_SHOW','PERCENTAGE']
    eth_tmap = eth_tmap[cols]
    eth_tmap = capitalize_column(eth_tmap,'Symbols')
    eth_tmap = change_name(eth_tmap,'Symbols')
    if label=="Deposit":
        deposit = eth_tmap[eth_tmap['PERCENTAGE'] == eth_tmap['PERCENTAGE'].max()]
        return deposit.to_dict(orient="records")
    elif label=="Withdraw":
        withdraw = eth_tmap[eth_tmap['PERCENTAGE'] == eth_tmap['PERCENTAGE'].min()]
        return withdraw.to_dict(orient="records")
    else:
        return f'Not found: {label} please choose [ Deposit, Withdraw] '
    
# netflow of each balance
@eth_router.get('/eth/netflow')
async def ETH_netflow(balance:str,start:str,end:str):
    choice_condition = ['all','Binance', 'Bitfinex', 'Kraken', 'OKX', 'Gemini', 'Crypto.com','Bybit', 'Bithumb', 'Kucoin', 'Gate', 'Coinone', 'Houbi','Bitflyer', 'Korbit', 'Binance US', 'Coinbase', 'MEXC', 'Idex','Bitmex']
    if balance not in choice_condition:
        return f'balance: {balance} is not found, plase choice another ["all","Binance", "Bitfinex", "Kraken", "OKX", "Gemini", "Crypto.com","Bybit", "Bithumb", "Kucoin", "Gate", "Coinone", "Houbi","Bitflyer", "Korbit", "Binance US", "Coinbase", "MEXC", "Idex","Bitmex"]'
    
    elif balance == 'all':
        df_total_line =ETH_psql.groupby(['time','price'])[['value']].agg({'value':'sum'}).reset_index()
        df_total_line['last_vl'] = df_total_line['value'].shift(1).fillna(0)
        df_total_line = df_total_line.iloc[1:]
        df_total_line['netflow']= round(df_total_line['value']- df_total_line['last_vl'],2)
        df_total_line['money'] = round(df_total_line['price']*df_total_line['netflow'],2)
        df_total_line['money'] = df_total_line['money'].map(lambda x : abs(x))
        df_total_line['time_select'] = pd.to_datetime(df_total_line['time']).dt.date
        df_total_line['time_select'] = pd.to_datetime(df_total_line['time_select'])
        df_total_line['label']= 'all'
        cols = ['time','label','netflow','price','money']
        df_total_line = df_total_line[cols].rename(columns={'time':'timestamp'})
        return df_total_line.to_dict(orient='records')
        
    else:
        top1 =ETH_psql[ETH_psql['balance'].isin([balance])]
        
        top1['last_vl'] = top1['value'].shift(1).fillna(0)
        top1 = top1.iloc[1:]
        top1['netflow']= round(top1['value']- top1['last_vl'],2)
        top1['money'] = round(top1['price']*top1['netflow'],2)
        top1['time_select'] = pd.to_datetime(top1['time']).dt.date
        top1['time_select'] = pd.to_datetime(top1['time_select'])
        top1 = top1[top1['time_select'].between(start,end)]
        cols =['time','balance','netflow','price','money']
        top1 = top1[cols].rename(columns={'time':'timestamp','balance':'label'})
      
        return top1.to_dict(orient='records')
    
# #netflow all balance
# @eth_router.get('/eth/total_netflow')
# async def ETH_reserve_total(start:str,end:str):
#     df_total_line =ETH_psql.groupby(['time','price'])[['value']].agg({'value':'sum'}).reset_index()
#     df_total_line['money'] = round(df_total_line['price']*df_total_line['value'],2)
#     df_total_line['time_select'] = pd.to_datetime(df_total_line['time']).dt.date
#     df_total_line['time_select'] = pd.to_datetime(df_total_line['time_select'])
#     df_total_line = df_total_line[df_total_line['time_select'].between(start,end)]
#     cols = ['time','value','price','money']
#     df_total_line = df_total_line[cols].rename(columns={'time':'timestamp'})
#     return df_total_line.to_dict(orient='records')

#reserve each of balance
@eth_router.get('/eth/reserve')
async def ETH_reserve(balance:str,start:str,end:str):
    choice_condition = ['all','Binance', 'Bitfinex', 'Kraken', 'OKX', 'Gemini', 'Crypto.com','Bybit', 'Bithumb', 'Kucoin', 'Gate', 'Coinone', 'Houbi','Bitflyer', 'Korbit', 'Binance US', 'Coinbase', 'MEXC', 'Idex','Bitmex']
    if balance not in choice_condition:
        return f'balance: {balance} is not found, plase choice another ["all","Binance", "Bitfinex", "Kraken", "OKX", "Gemini", "Crypto.com","Bybit", "Bithumb", "Kucoin", "Gate", "Coinone", "Houbi","Bitflyer", "Korbit", "Binance US", "Coinbase", "MEXC", "Idex","Bitmex"]'
    elif balance=="all":
        df_total_line =ETH_psql.groupby(['time','price'])[['value']].agg({'value':'sum'}).reset_index()
        df_total_line['money'] = round(df_total_line['price']*df_total_line['value'],2)
        df_total_line['time_select'] = pd.to_datetime(df_total_line['time']).dt.date
        df_total_line['time_select'] = pd.to_datetime(df_total_line['time_select'])
        df_total_line = df_total_line[df_total_line['time_select'].between(start,end)]
        df_total_line['label'] = 'total reserve'
        cols = ['time','label','value','price','money']
        df_total_line = df_total_line[cols].rename(columns={'time':'timestamp'})
        return df_total_line.to_dict(orient='records')
    else:
        top1 =ETH_psql[ETH_psql['balance'].isin([balance])]
        top1['money'] = round(top1['value']*top1['price'],2)
        top1['time_select'] = pd.to_datetime(top1['time']).dt.date
        top1['time_select'] = pd.to_datetime(top1['time_select'])
        top1 = top1[top1['time_select'].between(start,end)]
        cols = ['time','balance','value','price','money']
        top1 = top1[cols].rename(columns={'time':'timestamp','balance':'label'})
        top1 = capitalize_column(top1,'label')
        return top1.to_dict(orient='records')
    
# Inflow In Ethereum
def create_netflow(balance:str):
    test = ETH_psql[ETH_psql['balance']==balance]
    test = test.sort_values(by=['time'],ascending=True)
    test['last_vl'] = test['value'].shift(1).fillna(0)
    test = test.iloc[1:]
    test['netflow']=round(test['value']-test['last_vl'],2)
    test['money'] = abs(test['price']* test['netflow'])
    cols = ['time','balance','netflow','money']
    test = test[cols]
    return test

def create_df_netflow():
    Binance = create_netflow('Binance')
    Gemini = create_netflow('Gemini')
    Idex = create_netflow('Idex')
    Kucoin = create_netflow('Kucoin')
    OKX = create_netflow('OKX')
    Bitmex = create_netflow('Bitmex')
    Kraken = create_netflow('Kraken')
    Bitflyer = create_netflow('Bitflyer')
    Coinone = create_netflow('Coinone')
    Korbit = create_netflow('Korbit')
    MEXC = create_netflow('MEXC')
    Bitfinex = create_netflow('Bitfinex')
    Gate = create_netflow('Gate')
    Binance_US = create_netflow('Binance US')
    Bybit = create_netflow('Bybit')
    Bithumb = create_netflow('Bithumb')
    Crypto_com = create_netflow('Crypto.com')
    Coinbase = create_netflow('Coinbase')
    Houbi = create_netflow('Houbi')
    data= pd.concat([Binance,Gemini,Idex,Kucoin,OKX,Bitmex,Kraken,Bitflyer,Coinone,Korbit,MEXC,Bitfinex,Gate,Binance_US,Bybit,Bithumb,Crypto_com,Coinbase,Houbi],axis=0)
    data = data.sort_values(by=['time'],ascending=True)
    return data

@eth_router.get('/eth/Inflow_exchange')
async def Inflow_exchange(start:str,end:str,label:str):
    choice_condition = ['Binance', 'Bitfinex', 'Kraken', 'OKX', 'Gemini', 'Crypto.com','Bybit', 'Bithumb', 'Kucoin', 'Gate', 'Coinone', 'Houbi','Bitflyer', 'Korbit', 'Binance US', 'Coinbase', 'MEXC', 'Idex','Bitmex']
    if label not in choice_condition:
        return f'balance: {label} is not found, plase choice another ["Binance", "Bitfinex", "Kraken", "OKX", "Gemini", "Crypto.com","Bybit", "Bithumb", "Kucoin", "Gate", "Coinone", "Houbi","Bitflyer", "Korbit", "Binance US", "Coinbase", "MEXC", "Idex","Bitmex"]'
    else:
        data = create_df_netflow()
        data = data[data['balance']==label]
        data = data[data['netflow']>0]
        data['time_select'] = pd.to_datetime(data['time']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols = ['time','balance','netflow','money']
        data =data[cols].rename(columns={'time':'timestamp','balance':'label','netflow':'value'})
        return data.to_dict(orient='records')

@eth_router.get('/eth/Outflow_exchange')
async def Outflow_exchange(start:str,end:str,label:str):
    choice_condition = ['Binance', 'Bitfinex', 'Kraken', 'OKX', 'Gemini', 'Crypto.com','Bybit', 'Bithumb', 'Kucoin', 'Gate', 'Coinone', 'Houbi','Bitflyer', 'Korbit', 'Binance US', 'Coinbase', 'MEXC', 'Idex','Bitmex']
    if label not in choice_condition:
        return f'balance: {label} is not found, plase choice another ["Binance", "Bitfinex", "Kraken", "OKX", "Gemini", "Crypto.com","Bybit", "Bithumb", "Kucoin", "Gate", "Coinone", "Houbi","Bitflyer", "Korbit", "Binance US", "Coinbase", "MEXC", "Idex","Bitmex"]'
    else:
        data = create_df_netflow()
        data = data[data['balance']==label]
        data = data[data['netflow']<0]
        data['time_select'] = pd.to_datetime(data['time']).dt.date
        data['time_select'] = pd.to_datetime(data['time_select'])
        data = data[data['time_select'].between(start,end)]
        cols = ['time','balance','netflow','money']
        data =data[cols].rename(columns={'time':'timestamp','balance':'label','netflow':'value'})
        return data.to_dict(orient='records')

@eth_router.get('/eth/NetFlow_exchange')
async def Netflow_exchange(start:str,end:str):
    data = create_df_netflow()
    data['time_select'] = pd.to_datetime(data['time']).dt.date
    data['time_select'] = pd.to_datetime(data['time_select'])
    data = data[data['time_select'].between(start,end)]
    cols = ['time','balance','netflow','money']
    data =data[cols].rename(columns={'time':'timestamp','balance':'label','netflow':'value'})
    return data.to_dict(orient='records')