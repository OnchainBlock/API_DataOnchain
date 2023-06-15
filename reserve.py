import sys
sys.path.append('/Users/dev/Thang_DataEngineer/Fast_api')
from imports import *
my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
data = pd.read_sql(query_cex, my_server)
data['TimeStamp'] = data['TimeStamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
data['SUM'] = data['USDT'] + data['USDC']+data['BUSD']

data = data[['TimeStamp', 'Symbols', 'SUM']]
data = data[data['TimeStamp'] == data['TimeStamp'].max()]


