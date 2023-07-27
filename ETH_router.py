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
    
# #reserve all balance
# @eth_router.get('/eth/reserve_total')
# async def ETH_netflow_total(start:str,end:str):
#     df_total_line =ETH_psql.groupby(['time','price'])[['value']].agg({'value':'sum'}).reset_index()
#     df_total_line['last_vl'] = df_total_line['value'].shift(1).fillna(0)
#     df_total_line = df_total_line.iloc[1:]
#     df_total_line['netflow']= round(df_total_line['value']- df_total_line['last_vl'],2)
#     df_total_line['money'] = round(df_total_line['price']*df_total_line['netflow'],2)
#     df_total_line['time_select'] = pd.to_datetime(df_total_line['time']).dt.date
#     df_total_line['time_select'] = pd.to_datetime(df_total_line['time_select'])
#     df_total_line = df_total_line[df_total_line['time_select'].between(start,end)]
#     cols = ['time','netflow','price','money']
#     df_total_line = df_total_line[cols].rename(columns={'time':'timestamp'})
#     return df_total_line.to_dict(orient='records')

