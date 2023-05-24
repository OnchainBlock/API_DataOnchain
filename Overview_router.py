from imports import *
# from Overview_data.Cex import data
from Overview_data.Cex import data
from Overview_data.Dexx import DAI_pie_df, LUSD_pie, Tusd_pie, DAI, LUSD, TUSD
from Distribution_Data.Bridge_data import TOTAL_MULTICHAIN,Celer_cBridge,HOP,STARGATE,SYNAPSE
from Distribution_Data.Bridge_data import *
from Overview_data.Dexx import *


overview_router = APIRouter(
    prefix='/overview',
    tags=['overview'],

)


@overview_router.get('/Cex')
async def choice_time(start: str, end: str, label: str):
    labels = data[data['Name'] == label]
    if labels.empty:
        return {f'Label "{label}" not found plase choice: ["Binance","OKX","Kucoin","Crypto.com","MEXC","Coinbase","Gate","Bitmex","Bitfinex","Houbi","Bittrex","FTX","Binance US","Coinlist","Bitstamp","FTX US"]'}
    else:
        data_json = data[data['TimeStamp'].between(start, end)]
        data_json = data_json[data_json['Name'] == label]
        return data_json.to_dict(orient='records')


@overview_router.get('/Cex/pie')
async def pie_day():
    pie_df = data[data['TimeStamp'] == data['TimeStamp'].max()]
    pie_df = pie_df.sort_values(by='Value', ascending=False)
    others = pie_df[4:]
    others = pd.DataFrame({
        'TimeStamp': others['TimeStamp'].unique(),
        'Name': ['Others'],
        'Value': others['Value'].sum()
    })
    create_df = pd.concat([pie_df, others], ignore_index=True)
    create_df = create_df.drop(
        create_df[create_df['Value'] == 0.].index)
    return create_df.to_dict(orient='records')

# DEX


@overview_router.get('/Dex/pie')
async def pie_date(label: str):
    if label == 'Dai':
        return DAI_pie_df.to_dict(orient='records')
    elif label == 'Lusd':
        return LUSD_pie.to_dict(orient='records')
    elif label == 'Tusd':
        return Tusd_pie.to_dict(orient='records')
    else:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}


@overview_router.get('/Dex/')
async def choice_time(start: str, end: str, label: str):
    if label == 'Lusd':
        LUSD_line = lusd_line(LUSD)
        return LUSD_line.to_dict(orient='records')
    elif label == 'Dai':
        dai_df = Dai_line(DAI)
        return dai_df.to_dict(orient='records')
    elif label == 'Tusd':
        tusd_df = Tusd_line(TUSD)
        return tusd_df.to_dict(orient='records')

    else:
        return {'status': 'fail', 'message': f'Label "{label}" not found.'}


# Bridge Overviews
@overview_router.get('/Bridge/pie')
async def create_bridge_pie():
    multichain = TOTAL_MULTICHAIN[TOTAL_MULTICHAIN['TIMESTAMP']==TOTAL_MULTICHAIN['TIMESTAMP'].max()]
    celer = Celer_cBridge[Celer_cBridge['TIMESTAMP']==Celer_cBridge['TIMESTAMP'].max()]
    hop = HOP[HOP['TIMESTAMP']==HOP['TIMESTAMP'].max()]
    stargate = STARGATE[STARGATE['TIMESTAMP']==STARGATE['TIMESTAMP'].max()]
    synapse = SYNAPSE[SYNAPSE['TIMESTAMP']==SYNAPSE['TIMESTAMP'].max()]
    df_mul = pd.DataFrame({
        'BRIDGE':['Multichain bridge','Celer cBridge','Hop bridge','Stargate bridge','Synapse bridge'],
        'VALUE':[multichain['VALUE'].sum(),celer['VALUE'].sum(),hop['VALUE'].sum(),stargate['VALUE'].sum(),synapse['VALUE'].sum()]
    })
    return df_mul.to_dict(orient='records')


# đoạn này sẽ có input ;{start}{end}{Bridge_name}:
@overview_router.get('/Bridge/')
async def choice_bridge(start:str, end:str,label:str):
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if label not in choice_condition:
        return f'label: {label} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
    elif label =="Multichain":
        TOTAL_ASSETS_MULTICHAIN = create_multichain(TOTAL_MULTICHAIN)
        TOTAL_ASSETS_MULTICHAIN = TOTAL_ASSETS_MULTICHAIN[TOTAL_ASSETS_MULTICHAIN['TIMESTAMP'].between(start,end)]
        return TOTAL_ASSETS_MULTICHAIN.to_dict(orient='records')
    elif label =="Celer":
        TOTAL_ASSETS_CELER = create_celer(Celer_cBridge)
        TOTAL_ASSETS_CELER = TOTAL_ASSETS_CELER[TOTAL_ASSETS_CELER['TIMESTAMP'].between(start,end)]
        return TOTAL_ASSETS_CELER.to_dict(orient='records')
    elif label =='Hop':
        TOTAL_ASSETS_HOP = create_hop(HOP)
        TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP[TOTAL_ASSETS_HOP['TIMESTAMP'].between(start,end)]
        return TOTAL_ASSETS_HOP.to_dict(orient='records')
    elif label=='Stargate':
        TOTAL_ASSETS_STARGATE= create_starage(STARGATE)
        TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE[TOTAL_ASSETS_STARGATE['TIMESTAMP'].between(start,end)]
        return TOTAL_ASSETS_STARGATE.to_dict(orient='records')
    elif label=='Synapse':
        TOTAL_ASSETS_SYNAPSE = create_synapse(SYNAPSE)
        TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE[TOTAL_ASSETS_SYNAPSE['TIMESTAMP'].between(start,end)]
        return TOTAL_ASSETS_SYNAPSE.to_dict(orient='records')

