from imports import *
my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
data = pd.read_sql(query_cex, my_server)
data['TimeStamp'] = data['TimeStamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
QK_Data = data.set_index('TimeStamp')
QK_Data = QK_Data.between_time('6:00', '10:59')
QK_Data = QK_Data.reset_index()
data['Value'] = data['USDT'] + data['USDC']+data['BUSD']

data = data[['TimeStamp', 'Symbols', 'Value']].rename(columns={'TimeStamp':'timestamp','Symbols':'label','Value':'value'})

