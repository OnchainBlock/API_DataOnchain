import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *

query_cexv1 = os.environ['query_cexall']
server = os.environ['my_server']

cex_df = pd.read_sql(query_cexv1,server)
cex_df['balance'] = cex_df['balance'].map(lambda x: x.capitalize())
token = cex_df.groupby(['timestamp','balance','token'])['value'].sum().reset_index()
token['timestamp'] = token['timestamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
now_token = token[token['timestamp']==token['timestamp'].max()][['balance','token','value']]

def create_line_df(balance:str,token_choice:str, start:str, end:str):
    choice_condition = ['all','usdt','usdc','busd','dai','lusd','tusd']
    choice_balance = ['Binance', 'Okx', 'Kucoin', 'Kraken', 'Bybit', 'Mexc', 'Bitget',
       'Gate', 'Crypto.com', 'Coinbase', 'Bitfinex', 'Gemini', 'Huobi']
    if token_choice not in choice_condition or balance not in choice_balance:
        return f'balance: {token_choice} is not found, plase choice another ["all","usdt","usdc","busd","dai","lusd","tusd"]', f'balance: {balance} is not found, plase choice another ["Binance", "Okx", "Kucoin", "Kraken", "Bybit", "Mexc", "Bitget","Gate", "Crypto.com", "Coinbase", "Bitfinex", "Gemini", "Huobi"]'
   
    elif token_choice =='all':
        cex_all = cex_df.groupby(['timestamp','balance']).agg({'value':'sum'}).reset_index().sort_values(by=['timestamp'],ascending=True)
        cex_all = cex_all[cex_all['balance']==balance]
        cex_all['time_select'] = pd.to_datetime(cex_all['timestamp']).dt.date
        cex_all['time_select'] = pd.to_datetime(cex_all['time_select'])
        cex_all['token'] ='all'
        token_condition = cex_all[cex_all['time_select'].between(start,end)]

        cols =['timestamp','balance','token','value']
        token_condition = token_condition[cols]
        return token_condition.to_dict(orient='records')
    else:
        data = cex_df[cex_df['balance']==balance]
        token = data.groupby(['timestamp','balance','token']).agg({'value':'sum'}).reset_index()
        token_condition = token[token['token']==token_choice].sort_values(by=['timestamp'],ascending=True)
        #choice date
        token_condition['time_select'] = pd.to_datetime(token_condition['timestamp']).dt.date
        token_condition['time_select'] = pd.to_datetime(token_condition['time_select'])
        token_condition = token_condition[token_condition['time_select'].between(start,end)]
        cols =['timestamp','balance','token','value']
        token_condition = token_condition[cols]
        return token_condition.to_dict(orient='records')




















































































































































