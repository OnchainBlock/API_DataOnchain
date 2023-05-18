from imports import *
from Distribution_Data.Cex_distribution import *

my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
data = pd.read_sql(query_cex, my_server)
data['TimeStamp'] = data['TimeStamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
QK_Data = data.set_index('TimeStamp')
QK_Data = QK_Data.between_time('6:00', '10:59')
QK_Data = QK_Data.reset_index()

distribution_router = APIRouter(
    prefix='/distribution',
    tags=['distribution : 4h, Day, Week, Month']
)

# CEX


@distribution_router.get('/Cex/Day')
async def stable():
    day_df = create_df_Treemap(QK_Data, data, 1)
    return day_df.to_dict(orient='records')


@distribution_router.get('/Cex/Week')
async def stable():
    week_df = create_df_Treemap(QK_Data, data, 7)
    return week_df.to_dict(orient='records')


@distribution_router.get('/Cex/Month')
async def stable():
    month_df = create_df_Treemap(QK_Data, data, 30)
    return month_df.to_dict(orient='records')


@distribution_router.get('/Cex/Day/Total')
async def total():
    day_df_total = create_df_Treemap_sum(QK_Data, data, 1)
    return day_df_total.to_dict(orient='records')


@distribution_router.get('/Cex/Week/Total')
async def total():
    week_df_total = create_df_Treemap_sum(QK_Data, data, 7)
    return week_df_total.to_dict(orient='records')


@distribution_router.get('/Cex/Month/Total')
async def total():
    Month_df_total = create_df_Treemap_sum(QK_Data, data, 30)
    return Month_df_total.to_dict(orient='records')


# Bridge


@distribution_router.get('/Bridge/Multichain/Day/usdt')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Day/usdc')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Day/busd')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Week/usdt')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Week/usdc')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Week/busd')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Month/usdt')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Month/usdc')
async def Multichain():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Multichain/Month/busd')
async def Multichain():
    return {'message': 'Distribution Cex '}

# Bridge Hop


@distribution_router.get('/Bridge/Hop/Day')
async def Hop():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Hop/Week')
async def Hop():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Hop/Month')
async def Hop():
    return {'message': 'Distribution Cex '}

# bridge stargate


@distribution_router.get('/Bridge/Stargate/Day')
async def Stargate():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Stargate/Week')
async def Stargate():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Stargate/Month')
async def Stargate():
    return {'message': 'Distribution Cex '}

# Bridge Celer


@distribution_router.get('/Bridge/Celer/Day')
async def Celer():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Celer/Week')
async def Celer():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Celer/Month')
async def Celer():
    return {'message': 'Distribution Cex '}

# Bridge Synapse


@distribution_router.get('/Bridge/Synapse/Day')
async def Synapse():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Synapse/Week')
async def Synapse():
    return {'message': 'Distribution Cex '}


@distribution_router.get('/Bridge/Synapse/Month')
async def Synapse():
    return {'message': 'Distribution Cex '}
