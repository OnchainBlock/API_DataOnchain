from imports import *
# from Overview_data.Cex import data
from Overview_data.Cex import data
from Overview_data.Dexx import DAI_pie_df, LUSD_pie_df, Tusd_pie, DAI, LUSD, TUSD, Dex_pie
from Distribution_Data.Bridge_data import TOTAL_MULTICHAIN,Celer_cBridge,HOP,STARGATE,SYNAPSE,Bridge_line
from Distribution_Data.Bridge_data import *
from Overview_data.Dexx import *
from UpgradeV1_data.dexc_data import *

import pandas as pd
overview_router = APIRouter(
    prefix='/overview',
    tags=['overview'],

)


@overview_router.get('/Cex')

async def choice_time(start: str, end: str, label: str):
    labels = data[data['label'] == label]
    if labels.empty:
        return {f'Label "{label}" not found plase choice: ["Binance","OKX","Kucoin","Crypto.com","MEXC","Coinbase","Gate","Bitmex","Bitfinex","Houbi","Bittrex","FTX","Binance US","Coinlist","Bitstamp","FTX US"]'}
    else:
        data['TIME'] = pd.to_datetime(data['timestamp']).dt.date
        data['TIME'] = pd.to_datetime(data['TIME'])
        data_json = data[data['TIME'].between(start, end)].drop(columns=['TIME'])
        data_json = data_json[data_json['label'] == label]
        return data_json.to_dict(orient='records')


@overview_router.get('/Cex/pie')
async def pie_day():
    pie_df = data[data['timestamp'] == data['timestamp'].max()]
    pie_df = pie_df.sort_values(by='value', ascending=False)
    # others = pie_df[4:]
    # others = pd.DataFrame({
    #     'timestamp': others['timestamp'].unique(),
    #     'label': ['Others'],
    #     'value': others['value'].sum()
    # })
    # create_df = pd.concat([pie_df, others], ignore_index=True)
    pie_df = pie_df.drop(
        pie_df[pie_df['value'] == 0.].index)
    pie_df = pie_df.sort_values(by='value', ascending=False)
    return pie_df.to_dict(orient='records')

# DEX


# @overview_router.get('/Dex/pie')
# async def pie_date(label: str):
#     if label == 'Dai':
#         return DAI_pie_df.to_dict(orient='records')
#     elif label == 'Lusd':
#         return LUSD_pie.to_dict(orient='records')
#     elif label == 'Tusd':
#         return Tusd_pie.to_dict(orient='records')
#     else:
#         return {'status': 'fail', 'message': f'Label "{label}" not found.'}
@overview_router.get('/Dex/pie')
async def pie_date(label: str):
    if label == 'Dai':
        return DAI_pie_df.to_dict(orient='records')
    elif label == 'Lusd':
        return LUSD_pie_df.to_dict(orient='records')
    elif label == 'Tusd':
        return Tusd_pie.to_dict(orient='records')
    else:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}

@overview_router.get('/Dex/pie/total')
async def pie_data():
    return Dex_pie.to_dict(orient='records')

@overview_router.get('/Dex')
async def choice_time(start: str, end: str, label: str):
    if label == 'Lusd':
        LUSD_line = lusd_line(LUSD)
        LUSD_line['time']= pd.to_datetime(LUSD_line['TIMESTAMP']).dt.date
        LUSD_line['time'] = pd.to_datetime(LUSD_line['time'])
        LUSD_line = LUSD_line[LUSD_line['time'].between(start,end)].drop(columns={'time'})
        LUSD_line = LUSD_line.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value'})
        return LUSD_line.to_dict(orient='records')
    elif label == 'Dai':
        dai_df = Dai_line(DAI)
        dai_df['time']= pd.to_datetime(dai_df['TIMESTAMP']).dt.date
        dai_df['time'] = pd.to_datetime(dai_df['time'])
        dai_df = dai_df[dai_df['time'].between(start,end)].drop(columns={'time'})
        dai_df = dai_df.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value'})
        return dai_df.to_dict(orient='records')
    elif label == 'Tusd':
        tusd_df = Tusd_line(TUSD)
        tusd_df['time']= pd.to_datetime(tusd_df['TIMESTAMP']).dt.date
        tusd_df['time'] = pd.to_datetime(tusd_df['time'])
        tusd_df = tusd_df[tusd_df['time'].between(start,end)].drop(columns={'time'})
        tusd_df = tusd_df.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value'})
        return tusd_df.to_dict(orient='records')

    else:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}
@overview_router.get('/Bridge/pie')
async def create_bridge_pie():
    multichain = Bridge_line[Bridge_line['TIMESTAMP']==Bridge_line['TIMESTAMP'].max()]
    celer = Celer_cBridge[Celer_cBridge['TIMESTAMP']==Celer_cBridge['TIMESTAMP'].max()]
    hop = HOP[HOP['TIMESTAMP']==HOP['TIMESTAMP'].max()]
    stargate = STARGATE[STARGATE['TIMESTAMP']==STARGATE['TIMESTAMP'].max()]
    synapse = SYNAPSE[SYNAPSE['TIMESTAMP']==SYNAPSE['TIMESTAMP'].max()]
    df_mul = pd.DataFrame({
        'label':['Multichain','Celer','Hop','Stargate','Synapse'],
        'name':[multichain['VALUE'].sum(),celer['VALUE'].sum(),hop['VALUE'].sum(),stargate['VALUE'].sum(),synapse['VALUE'].sum()]
    })
    return df_mul.to_dict(orient='records')


# đoạn này sẽ có input ;{start}{end}{Bridge_name}:
@overview_router.get('/Bridge')
async def choice_bridge(start:str, end:str,label:str):
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if label not in choice_condition:
        return f'label: {label} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
    elif label =="Multichain":
        TOTAL_ASSETS_MULTICHAIN = create_multichain(TOTAL_MULTICHAIN)
        TOTAL_ASSETS_MULTICHAIN = TOTAL_ASSETS_MULTICHAIN.groupby(['TIMESTAMP'])['VALUE'].sum().reset_index()
        TOTAL_ASSETS_MULTICHAIN['TIME'] = pd.to_datetime(TOTAL_ASSETS_MULTICHAIN['TIMESTAMP']).dt.date
        TOTAL_ASSETS_MULTICHAIN['TIME'] = pd.to_datetime(TOTAL_ASSETS_MULTICHAIN['TIME'])
        TOTAL_ASSETS_MULTICHAIN = TOTAL_ASSETS_MULTICHAIN[TOTAL_ASSETS_MULTICHAIN['TIME'].between(start,end)].drop(columns=['TIME'])
        TOTAL_ASSETS_MULTICHAIN = TOTAL_ASSETS_MULTICHAIN.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
        return TOTAL_ASSETS_MULTICHAIN.to_dict(orient='records')
    elif label =="Celer":
        TOTAL_ASSETS_CELER = create_celer(Celer_cBridge)
        TOTAL_ASSETS_CELER = TOTAL_ASSETS_CELER.groupby(['TIMESTAMP'])['VALUE'].sum().reset_index()
        TOTAL_ASSETS_CELER['TIME'] = pd.to_datetime(TOTAL_ASSETS_CELER['TIMESTAMP']).dt.date
        TOTAL_ASSETS_CELER['TIME'] = pd.to_datetime(TOTAL_ASSETS_CELER['TIME'])
        TOTAL_ASSETS_CELER = TOTAL_ASSETS_CELER[TOTAL_ASSETS_CELER['TIME'].between(start,end)].drop(columns=['TIME'])
        TOTAL_ASSETS_CELER  = TOTAL_ASSETS_CELER.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
        return TOTAL_ASSETS_CELER.to_dict(orient='records')
    elif label =='Hop':
        TOTAL_ASSETS_HOP = create_hop(HOP)
        TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP.groupby(['TIMESTAMP'])['VALUE'].sum().reset_index()
        TOTAL_ASSETS_HOP['TIME'] = pd.to_datetime(TOTAL_ASSETS_HOP['TIMESTAMP']).dt.date
        TOTAL_ASSETS_HOP['TIME'] = pd.to_datetime(TOTAL_ASSETS_HOP['TIME'])
        TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP[TOTAL_ASSETS_HOP['TIME'].between(start,end)].drop(columns=['TIME'])
        TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
        return TOTAL_ASSETS_HOP.to_dict(orient='records')
    elif label=='Stargate':
        TOTAL_ASSETS_STARGATE= create_starage(STARGATE)
        TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE.groupby(['TIMESTAMP'])['VALUE'].sum().reset_index()
        TOTAL_ASSETS_STARGATE['TIME'] = pd.to_datetime(TOTAL_ASSETS_STARGATE['TIMESTAMP']).dt.date
        TOTAL_ASSETS_STARGATE['TIME'] = pd.to_datetime(TOTAL_ASSETS_STARGATE['TIME'])
        TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE[TOTAL_ASSETS_STARGATE['TIME'].between(start,end)].drop(columns=['TIME'])
        TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
        return TOTAL_ASSETS_STARGATE.to_dict(orient='records')
    elif label=='Synapse':
        TOTAL_ASSETS_SYNAPSE = create_synapse(SYNAPSE)
        TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE.groupby(['TIMESTAMP'])['VALUE'].sum().reset_index()
        TOTAL_ASSETS_SYNAPSE['TIME'] = pd.to_datetime(TOTAL_ASSETS_SYNAPSE['TIMESTAMP']).dt.date
        TOTAL_ASSETS_SYNAPSE['TIME'] = pd.to_datetime(TOTAL_ASSETS_SYNAPSE['TIME'])
        TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE[TOTAL_ASSETS_SYNAPSE['TIME'].between(start,end)].drop(columns=['TIME'])
        TOTAL_ASSETS_SYNAPSE= TOTAL_ASSETS_SYNAPSE.rename(columns={'TIMESTAMP':'timestamp','VALUE':'value','EXPLORER':'label'})
        return TOTAL_ASSETS_SYNAPSE.to_dict(orient='records')

@overview_router.get('/updatetime')
async def uptime():
    return dict({'time':data['timestamp'].max()})

@overview_router.get('/dexc_statisc')
async def statistic_dexc(token:str):
    choice_condition = ['Dai','Lusd','Tusd']
    if token not in choice_condition:
        return f'balance: {token} is not found, plase choice another ["Dai","Lusd","Tusd"]'
    elif token=="Dai":
        return create_table(DAI)
    elif token=='Lusd':
        return create_table(LUSD)
    elif token=='Tusd':
        return create_table_tusd(TUSD)

# Statictis Bridge Stablecoin
@overview_router.get('/bridge_statistics')
async def statistic_bridge_st(bridge:str):
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if bridge not in choice_condition:
        return f'bridge: {bridge} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
    elif bridge =="Multichain":
        return create_table_bridge_st(multichain_table)
    elif bridge=='Celer':
        return create_table_bridge_st(celer_table)
    elif bridge=='Hop':
        return create_table_bridge_st(hop_table)
    elif bridge=='Stargate':
        return create_table_bridge_st(stargate_table)
    elif bridge=='Synapse':
        return create_table_bridge_st(synapse_table)
    
@overview_router.get('/eachofbridge_statisc')
async def balanceofstatisc_eachofbridge(bridge:str,token:str):
    choice_token = ['USDT','USDC','BUSD']
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if bridge not in choice_condition or token not in choice_token:
        return f'label: {bridge} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]\n token: {token} is not found choice ["USDT","USDC","BUSD"]'
    # multichain
    elif bridge=='Multichain' and token=='USDT':
        return create_table_statis_eachofbridge(choice_df_statics('Multichain','USDT'))
    elif bridge=='Multichain' and token=='USDC':
        return create_table_statis_eachofbridge(choice_df_statics('Multichain','USDC'))
    elif bridge=='Multichain' and token=='BUSD':
        return create_table_statis_eachofbridge(choice_df_statics('Multichain','BUSD'))
    # Celer
    elif bridge=='Celer' and token=='USDT':
        return create_table_statis_eachofbridge(choice_df_statics('Celer','USDT'))
    elif bridge=='Celer' and token=='USDC':
        return create_table_statis_eachofbridge(choice_df_statics('Celer','USDC'))
    elif bridge=='Celer' and token=='BUSD':
        return create_table_statis_eachofbridge(choice_df_statics('Celer','BUSD'))
    #Hop
    elif bridge=='Hop' and token=='USDT':
        return create_table_statis_eachofbridge(choice_df_statics('Hop','USDT'))
    elif bridge=='Hop' and token=='USDC':
        return create_table_statis_eachofbridge(choice_df_statics('Hop','USDC'))
    elif bridge=='Hop' and token=='BUSD':
        return create_table_statis_eachofbridge(choice_df_statics('Hop','BUSD'))
    #Stargate
    elif bridge=='Stargate' and token=='USDT':
        return create_table_statis_eachofbridge(choice_df_statics('Stargate','USDT'))
    elif bridge=='Stargate' and token=='USDC':
        return create_table_statis_eachofbridge(choice_df_statics('Stargate','USDC'))
    elif bridge=='Stargate' and token=='BUSD':
        return create_table_statis_eachofbridge(choice_df_statics('Stargate','BUSD'))
    #Synapse
    elif bridge=='Synapse' and token=='USDT':
        return create_table_statis_eachofbridge(choice_df_statics('Synapse','USDT'))
    elif bridge=='Synapse' and token=='USDC':
        return create_table_statis_eachofbridge(choice_df_statics('Synapse','USDC'))
    elif bridge=='Synapse' and token=='BUSD':
        return create_table_statis_eachofbridge(choice_df_statics('Synapse','BUSD'))