from imports import *
from Distribution_Data.Cex import *
from Distribution_Data.Bridge_data import TOTAL_MULTICHAIN,Celer_cBridge,HOP,STARGATE,SYNAPSE
from Distribution_Data.Bridge_data import *
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
    tags=['distribution']
)

# CEX


@distribution_router.get('/Cex')
async def hightlight_exchange(chioce_days: int, label: str):
    Hientai_Data = data[data['TimeStamp'] == data['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']

    QK_Data['TimeStamp'] = pd.to_datetime(QK_Data['TimeStamp']).dt.date

    Last_data = QK_Data[(QK_Data['TimeStamp'] == QK_Data['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
                        ][['USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
    Last_data = Last_data.reset_index()
    Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
        Last_data['USDC_Las'] + Last_data['BUSD_Las']

    DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE[f'{chioce_days}D_USDT'] = (
        (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_USDC'] = (
        (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_BUSD'] = (
        (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_ALL'] = (
        (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_LAS'])*100
    DATA_CHANGE = DATA_CHANGE.fillna(0)

    DATA_CHANGE_SUM = DATA_CHANGE[['Symbols',
                                   'ALL_HIENTAI', f'{chioce_days}D_ALL']]
    DATA_CHANGE_SUM['VALUE_SHOW'] = DATA_CHANGE_SUM['ALL_HIENTAI'].map(
        lambda x: numerize.numerize(x, 2))
    DATA_CHANGE_SUM.drop(
        DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(
        columns={f'{chioce_days}D_ALL': 'PERCENTAGE'})
    DATA_CHANGE_SUM['PERCENTAGE'] = DATA_CHANGE_SUM['PERCENTAGE'].map(
        lambda x: round(x, 2))
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.drop(
        DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(columns={'ALL_HIENTAI': 'VALUE'})
    if label == 'Deposit':
        result = DATA_CHANGE_SUM[DATA_CHANGE_SUM['PERCENTAGE']
                                 == DATA_CHANGE_SUM['PERCENTAGE'].max()]
        return result.to_dict(orient='records')

    elif label == 'Withdraw':
        result = DATA_CHANGE_SUM[DATA_CHANGE_SUM['PERCENTAGE']
                                 == DATA_CHANGE_SUM['PERCENTAGE'].min()]
        return result.to_dict(orient='records')
    else:
        return f'Not found: {label} please choose [ Deposit, Withdraw] '


@distribution_router.get('/Cex/Treemap')
async def Treemap(chioce_days: int, label: str):

    Hientai_Data = data[data['TimeStamp'] == data['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']

    QK_Data['TimeStamp'] = pd.to_datetime(QK_Data['TimeStamp']).dt.date

    Last_data = QK_Data[(QK_Data['TimeStamp'] == QK_Data['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
                        ][['USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
    Last_data = Last_data.reset_index()
    Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
        Last_data['USDC_Las'] + Last_data['BUSD_Las']

    DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE[f'{chioce_days}D_USDT'] = (
        (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_USDC'] = (
        (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_BUSD'] = (
        (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_ALL'] = (
        (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_LAS'])*100
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE_SUM = DATA_CHANGE[['Symbols',
                                   'ALL_HIENTAI', f'{chioce_days}D_ALL']]
    # DATA_CHANGE_SUM['VALUE_SHOW'] = DATA_CHANGE_SUM['ALL_HIENTAI'].map(
    #     lambda x: numerize.numerize(x, 2))
    DATA_CHANGE_SUM.drop(
        DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(
        columns={f'{chioce_days}D_ALL': 'PERCENTAGE'})
    DATA_CHANGE_SUM['PERCENTAGE'] = DATA_CHANGE_SUM['PERCENTAGE'].map(
        lambda x: round(x, 2))
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.drop(
        DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(columns={'ALL_HIENTAI': 'VALUE'})

    if label == 'Usdt':
        return Funtion_Col_Processing(DATA_CHANGE,  'Symbols', 'USDT', f'{chioce_days}D_USDT', 'USDT').rename(
            columns={'USDT': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']].to_dict(orient='records')

    elif label == 'Usdc':
        return Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'USDC', f'{chioce_days}D_USDC', 'USDC').rename(
            columns={'USDC': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']].to_dict(orient='records')

    elif label == 'Busd':
        return Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'BUSD', f'{chioce_days}D_BUSD', 'BUSD').rename(
            columns={'BUSD': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']].to_dict(orient='records')

    elif label == 'Total':
        return DATA_CHANGE_SUM.to_dict(orient='records')

    else:
        return f'Not found: {label} please choose [Usdt , Usdc, Busd, Total] '

@distribution_router.get('/Bridge/pie')
async def choice_bridge(label:str):
    cols = ['EXPLORER','VALUE']
    multichain = TOTAL_MULTICHAIN[TOTAL_MULTICHAIN['TIMESTAMP']==TOTAL_MULTICHAIN['TIMESTAMP'].max()]
    celer = Celer_cBridge[Celer_cBridge['TIMESTAMP']==Celer_cBridge['TIMESTAMP'].max()]
    hop = HOP[HOP['TIMESTAMP']==HOP['TIMESTAMP'].max()]
    stargate = STARGATE[STARGATE['TIMESTAMP']==STARGATE['TIMESTAMP'].max()]
    synapse = SYNAPSE[SYNAPSE['TIMESTAMP']==SYNAPSE['TIMESTAMP'].max()]
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if label not in choice_condition:
        return f'label: {label} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
    elif label=='Multichain':
        multichain = multichain[cols]
        return multichain.to_dict(orient='records')
    elif label =='Celer':
        celer = celer[cols]
        return celer.to_dict(orient='records')
    elif label=="Hop":
        hop = hop[cols]
        return hop.to_dict(orient='records')
    elif label=='Stargate':
        stargate = stargate[cols]
        return stargate.to_dict(orient='records')
    elif label=='Synapse':
        synapse = synapse[cols]
        return synapse.to_dict(orient='records')