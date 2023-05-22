from Distribution_Data.Cex import *
from fastapi import APIRouter
Reserve_router = APIRouter(
    prefix='/reserve',
    tags=['reserve']
)
my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
data = pd.read_sql(query_cex, my_server)
data['TimeStamp'] = data['TimeStamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
data['SUM'] = data['USDT'] + data['USDC']+data['BUSD']

data = data[['TimeStamp', 'Symbols', 'SUM']]


@Reserve_router.get('/tier1')
async def reserve(start: str, end: str):
    data_tier1 = data.loc[data['Symbols'].isin(
        ['OKX', 'Kucoin', 'MEXC', 'Crypto.com', 'Coinbase', 'Gate'])]
    n_labels = data_tier1['Symbols'].unique()

    data_tier1_json = data_tier1[data_tier1['TimeStamp'].between(
        start, end)]
    return data_tier1_json.to_dict(orient='records')


@Reserve_router.get('/tier2')
async def reserve(start: str, end: str):
    data_tier1 = data.loc[data['Symbols'].isin(
        ['Bitfinex', 'Bitmex', 'Houbi', 'Bittrex', 'FTX'])]
    n_labels = data_tier1['Symbols'].unique()

    data_tier1_json = data_tier1[data_tier1['TimeStamp'].between(
        start, end)]
    return data_tier1_json.to_dict(orient='records')


@Reserve_router.get('/tier3')
async def reserve(start: str, end: str):
    data_tier1 = data.loc[data['Symbols'].isin(
        ['Binance US', 'Coinlist', 'Bitstamp', 'FTX US'])]
    n_labels = data_tier1['Symbols'].unique()

    data_tier1_json = data_tier1[data_tier1['TimeStamp'].between(
        start, end)]
    return data_tier1_json.to_dict(orient='records')
