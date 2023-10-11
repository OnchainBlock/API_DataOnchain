from imports import *
from Distribution_Data.Cex import *
# from Distribution_Data.Bridge_data import TOTAL_MULTICHAIN,Celer_cBridge,HOP,STARGATE,SYNAPSE,multichain_pie,celer_pie,hop_pie,stargate_pie,synapse_pie,multichain_pie
# from Distribution_Data.Bridge_data import *
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
def hightlight_exchange(chioce_days: int, label: str):
    Hientai_Data = data[data['TimeStamp'] == data['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']

    QK_Data['TimeStamp'] = pd.to_datetime(QK_Data['TimeStamp']).dt.date

    Last_data = QK_Data[(QK_Data['TimeStamp'] == QK_Data['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
                        ][['Symbols','USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
    Last_data = Last_data.reset_index()
    Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
        Last_data['USDC_Las'] + Last_data['BUSD_Las']
    Last_data = Last_data.set_index('Symbols').reindex(Hientai_Data['Symbols']).reset_index()
    DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE[f'{chioce_days}D_USDT'] = (
        (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT'])*100
    DATA_CHANGE[f'{chioce_days}D_USDC'] = (
        (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC'])*100
    DATA_CHANGE[f'{chioce_days}D_BUSD'] = (
        (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD'])*100
    DATA_CHANGE[f'{chioce_days}D_ALL'] = (
        (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_HIENTAI'])*100
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


# @distribution_router.get('/Cex/Treemap')
# async def Treemap(chioce_days: int, label: str):
def Treemap(chioce_days:int, label:str):
    Hientai_Data = data[data['TimeStamp'] == data['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']
        

    QK_Data['TimeStamp'] = pd.to_datetime(QK_Data['TimeStamp']).dt.date

    Last_data = QK_Data[(QK_Data['TimeStamp'] == QK_Data['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
                        ][['Symbols','USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
    Last_data = Last_data.reset_index()
    Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
        Last_data['USDC_Las'] + Last_data['BUSD_Las']
    Last_data = Last_data.set_index('Symbols').reindex(Hientai_Data['Symbols']).reset_index()
    DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE[f'{chioce_days}D_USDT'] = (
        (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT'])*100
    DATA_CHANGE[f'{chioce_days}D_USDC'] = (
        (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC'])*100
    DATA_CHANGE[f'{chioce_days}D_BUSD'] = (
        (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD'])*100
    DATA_CHANGE[f'{chioce_days}D_ALL'] = (
        (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_HIENTAI'])*100
    
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

  
    if label =='Busd':
        cols_busd = ['Symbols','BUSD',f'{chioce_days}D_BUSD']
        BUSD = DATA_CHANGE[cols_busd]
        BUSD = BUSD.replace([np.inf, -np.inf], 0).fillna(0)
        BUSD = BUSD.drop(BUSD[BUSD[f'{chioce_days}D_BUSD'] == 0.00].index)
        BUSD[f'{chioce_days}D_BUSD'] = BUSD[f'{chioce_days}D_BUSD'].map(lambda x: round(x, 2))
        BUSD = BUSD.rename(columns={'BUSD':'VALUE',f'{chioce_days}D_BUSD':'PERCENTAGE'})
        BUSD['VL_CHANGE'] = abs(BUSD['VALUE']*(BUSD['PERCENTAGE']/100))
       
        BUSD = BUSD.sort_values(by = ['VALUE'], ascending=False)
        size = [600,180,80,40,32,26,20,16,12,10,10,6,6]
        BUSD['size'] = [i for i in size[:len(BUSD)]]
        BUSD = BUSD.fillna('')
        return BUSD.to_dict(orient='records')
    elif label=='Usdc':
        cols_usdc = ['Symbols','USDC',f'{chioce_days}D_USDC']
        USDC = DATA_CHANGE[cols_usdc]
        USDC = USDC.replace([np.inf, -np.inf], 0).fillna(0)
        USDC = USDC.drop(USDC[USDC[f'{chioce_days}D_USDC'] == 0.00].index)
        USDC[f'{chioce_days}D_USDC'] = USDC[f'{chioce_days}D_USDC'].map(lambda x: round(x, 2))
        USDC = USDC.rename(columns={'USDC':'VALUE',f'{chioce_days}D_USDC':'PERCENTAGE'})
        USDC['VL_CHANGE'] = abs(USDC['VALUE']*(USDC['PERCENTAGE']/100))
        USDC = USDC.sort_values(by = ['VALUE'], ascending=False)
        size = [600,180,80,40,32,26,20,16,12,10,10,6,6,6]
        USDC['size'] = [i for i in size[:len(USDC)]]
        
        return USDC.to_dict(orient='records')
    elif label=='Usdt':
        cols_usdt = ['Symbols','USDT',f'{chioce_days}D_USDT']
        USDT = DATA_CHANGE[cols_usdt]
        USDT = USDT.replace([np.inf, -np.inf], 0).fillna(0)
        USDT = USDT.drop(USDT[USDT[f'{chioce_days}D_USDT'] == 0.00].index)
        USDT[f'{chioce_days}D_USDT'] = USDT[f'{chioce_days}D_USDT'].map(lambda x: round(x, 2))
        USDT = USDT.rename(columns={'USDT':'VALUE',f'{chioce_days}D_USDT':'PERCENTAGE'})
        USDT['VL_CHANGE'] = abs(USDT['VALUE']*(USDT['PERCENTAGE']/100))
        USDT = USDT.sort_values(by = ['VALUE'], ascending=False)
        size = [600,180,80,40,32,26,20,16,12,10,10,6,6,6]
        USDT['size'] = [i for i in size[:len(USDT)]]
        USDT = USDT.fillna('')
        return USDT.to_dict(orient='records')
    elif label=="Total":
        DATA_CHANGE_SUM = DATA_CHANGE_SUM.replace([np.inf, -np.inf], 0).fillna(0)
        DATA_CHANGE_SUM = DATA_CHANGE_SUM.drop(DATA_CHANGE_SUM[DATA_CHANGE_SUM['PERCENTAGE'] == 0.00].index)
        DATA_CHANGE_SUM['VL_CHANGE'] = abs(DATA_CHANGE_SUM['VALUE']*(DATA_CHANGE_SUM['PERCENTAGE']/100))
        DATA_CHANGE_SUM = DATA_CHANGE_SUM.sort_values(by = ['VALUE'],ascending=False)
        size = [600,180,80,40,32,26,20,16,12,10,10,6,6,6]
        DATA_CHANGE_SUM['size'] = [i for i in size[:len(DATA_CHANGE_SUM)]]
        DATA_CHANGE_SUM = DATA_CHANGE_SUM.fillna('')
        return DATA_CHANGE_SUM.to_dict(orient='records')

# async def Treemap(chioce_days: int, label: str):

#     Hientai_Data = data[data['TimeStamp'] == data['TimeStamp'].max()][[
#         'Symbols', 'USDT', 'USDC', 'BUSD']]
#     Hientai_Data = Hientai_Data.reset_index()
#     Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
#         Hientai_Data['USDC'] + Hientai_Data['BUSD']


#     QK_Data['TimeStamp'] = pd.to_datetime(QK_Data['TimeStamp']).dt.date

#     Last_data = QK_Data[(QK_Data['TimeStamp'] == QK_Data['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
#                         ][['USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
#     Last_data = Last_data.reset_index()
#     Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
#         Last_data['USDC_Las'] + Last_data['BUSD_Las']

#     DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
#     DATA_CHANGE = DATA_CHANGE.fillna(0)
#     DATA_CHANGE[f'{chioce_days}D_USDT'] = (
#         (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT_Las'])*100
#     DATA_CHANGE[f'{chioce_days}D_USDC'] = (
#         (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC_Las'])*100
#     DATA_CHANGE[f'{chioce_days}D_BUSD'] = (
#         (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD_Las'])*100
#     DATA_CHANGE[f'{chioce_days}D_ALL'] = (
#         (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_LAS'])*100
#     DATA_CHANGE = DATA_CHANGE.fillna(0)
#     DATA_CHANGE_SUM = DATA_CHANGE[['Symbols',
#                                    'ALL_HIENTAI', f'{chioce_days}D_ALL']]
#     # DATA_CHANGE_SUM['VALUE_SHOW'] = DATA_CHANGE_SUM['ALL_HIENTAI'].map(
#     #     lambda x: numerize.numerize(x, 2))
#     DATA_CHANGE_SUM.drop(
#         DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
#     DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(
#         columns={f'{chioce_days}D_ALL': 'PERCENTAGE'})
#     DATA_CHANGE_SUM['PERCENTAGE'] = DATA_CHANGE_SUM['PERCENTAGE'].map(
#         lambda x: round(x, 2))
#     DATA_CHANGE_SUM = DATA_CHANGE_SUM.drop(
#         DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
#     DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(columns={'ALL_HIENTAI': 'VALUE'})

#     if label == 'Usdt':
#         return Funtion_Col_Processing(DATA_CHANGE,  'Symbols', 'USDT', f'{chioce_days}D_USDT', 'USDT').rename(
#             columns={'USDT': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']].to_dict(orient='records')

#     elif label == 'Usdc':
#         return Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'USDC', f'{chioce_days}D_USDC', 'USDC').rename(
#             columns={'USDC': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']].to_dict(orient='records')

#     elif label == 'Busd':
#         return Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'BUSD', f'{chioce_days}D_BUSD', 'BUSD').rename(
#             columns={'BUSD': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']].to_dict(orient='records')

#     elif label == 'Total':
#         return DATA_CHANGE_SUM.to_dict(orient='records')

#     else:
#         return f'Not found: {label} please choose [Usdt , Usdc, Busd, Total] '

@distribution_router.get('/Bridge/pie')
async def choice_bridge(label:str):
    cols = ['EXPLORER','VALUE']
    multichain = multichain_pie[multichain_pie['TIMESTAMP']==multichain_pie['TIMESTAMP'].max()]
    celer = celer_pie[celer_pie['TIMESTAMP']==celer_pie['TIMESTAMP'].max()]
    hop = hop_pie[hop_pie['TIMESTAMP']==hop_pie['TIMESTAMP'].max()]
    stargate = stargate_pie[stargate_pie['TIMESTAMP']==stargate_pie['TIMESTAMP'].max()]
    synapse = synapse_pie[synapse_pie['TIMESTAMP']==synapse_pie['TIMESTAMP'].max()]
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
    
# @distribution_router.get('/Bridge')
# async def choice_bridge(start:str, end:str,label:str):
#     choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
#     if label not in choice_condition:
#         return f'label: {label} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
#     elif label =="Multichain":
#         TOTAL_ASSETS_MULTICHAIN = create_multichain(TOTAL_MULTICHAIN)
#         TOTAL_ASSETS_MULTICHAIN['TIME'] = pd.to_datetime(TOTAL_ASSETS_MULTICHAIN['TIMESTAMP']).dt.date
#         TOTAL_ASSETS_MULTICHAIN['TIME'] = pd.to_datetime(TOTAL_ASSETS_MULTICHAIN['TIME'])
#         TOTAL_ASSETS_MULTICHAIN = TOTAL_ASSETS_MULTICHAIN[TOTAL_ASSETS_MULTICHAIN['TIME'].between(start,end)].drop(columns=['TIME'])
#         TOTAL_ASSETS_MULTICHAIN = TOTAL_ASSETS_MULTICHAIN.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
#         return TOTAL_ASSETS_MULTICHAIN.to_dict(orient='records')
#     elif label =="Celer":
#         TOTAL_ASSETS_CELER = create_celer(Celer_cBridge)
#         TOTAL_ASSETS_CELER['TIME'] = pd.to_datetime(TOTAL_ASSETS_CELER['TIMESTAMP']).dt.date
#         TOTAL_ASSETS_CELER['TIME'] = pd.to_datetime(TOTAL_ASSETS_CELER['TIME'])
#         TOTAL_ASSETS_CELER = TOTAL_ASSETS_CELER[TOTAL_ASSETS_CELER['TIME'].between(start,end)].drop(columns=['TIME'])
#         TOTAL_ASSETS_CELER  = TOTAL_ASSETS_CELER.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
#         return TOTAL_ASSETS_CELER.to_dict(orient='records')
#     elif label =='Hop':
#         TOTAL_ASSETS_HOP = create_hop(HOP)
#         TOTAL_ASSETS_HOP['TIME'] = pd.to_datetime(TOTAL_ASSETS_HOP['TIMESTAMP']).dt.date
#         TOTAL_ASSETS_HOP['TIME'] = pd.to_datetime(TOTAL_ASSETS_HOP['TIME'])
#         TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP[TOTAL_ASSETS_HOP['TIME'].between(start,end)].drop(columns=['TIME'])
#         TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
#         return TOTAL_ASSETS_HOP.to_dict(orient='records')
#     elif label=='Stargate':
#         TOTAL_ASSETS_STARGATE= create_starage(STARGATE)
#         TOTAL_ASSETS_STARGATE['TIME'] = pd.to_datetime(TOTAL_ASSETS_STARGATE['TIMESTAMP']).dt.date
#         TOTAL_ASSETS_STARGATE['TIME'] = pd.to_datetime(TOTAL_ASSETS_STARGATE['TIME'])
#         TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE[TOTAL_ASSETS_STARGATE['TIME'].between(start,end)].drop(columns=['TIME'])
#         TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
#         return TOTAL_ASSETS_STARGATE.to_dict(orient='records')
#     elif label=='Synapse':
#         TOTAL_ASSETS_SYNAPSE = create_synapse(SYNAPSE)
#         TOTAL_ASSETS_SYNAPSE['TIME'] = pd.to_datetime(TOTAL_ASSETS_SYNAPSE['TIMESTAMP']).dt.date
#         TOTAL_ASSETS_SYNAPSE['TIME'] = pd.to_datetime(TOTAL_ASSETS_SYNAPSE['TIME'])
#         TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE[TOTAL_ASSETS_SYNAPSE['TIME'].between(start,end)].drop(columns=['TIME'])
#         TOTAL_ASSETS_SYNAPSE= TOTAL_ASSETS_SYNAPSE.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
#         return TOTAL_ASSETS_SYNAPSE.to_dict(orient='records')
