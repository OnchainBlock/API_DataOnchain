import sys
sys.path.append(r'/root/API_DataOnchain')

from imports import *
my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
data = pd.read_sql(query_cex, my_server)
data['TimeStamp'] = data['TimeStamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
data = data[data['Symbols'].isin(['Binance', 'Coinbase', 'FTX US', 'Bitstamp', 'Gate', 'MEXC',
       'Binance US', 'CoinList', 'Crypto.com', 'Bitmex', 'Bitfinex',
       'FTX', 'Houbi', 'Kucoin', 'OKX', 'Bittrex', 'Coinlist'])]
QK_Data = data.set_index('TimeStamp')
QK_Data = QK_Data.between_time('6:00', '10:59')
QK_Data = QK_Data.reset_index()
data['Value'] = data['USDT'] + data['USDC']+data['BUSD']

data = data[['TimeStamp', 'Symbols', 'Value']].rename(columns={'TimeStamp':'timestamp','Symbols':'label','Value':'value'})

# Pie Funtions

def pie_day():
    pie_df = data[data['timestamp'] == data['timestamp'].max()]
    pie_df = pie_df.sort_values(by='value', ascending=False)
    pie_df = pie_df.drop(
        pie_df[pie_df['value'] == 0.].index)
    pie_df = pie_df.sort_values(by='value', ascending=False)
    return pie_df.to_dict(orient='records')


