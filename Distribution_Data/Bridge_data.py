# import sys
# sys.path.append('/Users/dev/Thang_DataEngineer/Fast_api')
# from imports import *

import sys
from fastapi import APIRouter
from fastapi.openapi.utils import get_openapi
from fastapi import FastAPI,Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from dotenv.main import load_dotenv
import os
import re
from typing import List
from pathlib import Path
import pandas as pd
import plotly.express as px
import numpy as np
from numerize import numerize
import datetime
import _datetime
from sqlalchemy import create_engine
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
pd.options.mode.chained_assignment = None
load_dotenv()

my_server = os.environ['my_server']
query_bridge = os.environ['query_multichain']

DF_MULTICHAIN = pd.read_sql(query_bridge, my_server)
'''FUNTION'''

# Funtions


def remove_number_string(df, str):
    return df[str].map(lambda x: re.sub('[0-9]', '', x))
# def filter data


def Filter_data_duplicateValue(df, label, explorer):
    ''' 
    CHỨC NĂNG: 
    - Sum value những Explorer trùng nhau

    '''
    tmp = df.loc[(df['LABEL'] == label) & (df['EXPLORER'] == explorer)]
    return pd.DataFrame(tmp.groupby(['TIMESTAMP', 'CHAIN', 'LABEL', 'EXPLORER'])['VALUE'].sum()).reset_index()


''' MERGE DATA HEADER & BODY'''

n_label = DF_MULTICHAIN['LABEL'].unique()
n_explorer = DF_MULTICHAIN['EXPLORER'].unique()
DF_MULTICHAIN['TIMESTAMP'] = DF_MULTICHAIN['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
DF_MULTICHAIN['TIMESTAMP'] = pd.to_datetime(DF_MULTICHAIN['TIMESTAMP'])

# ETH


def processing_Data(DF_MULTICHAIN, n_label, n_explorer):
    ETH_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[0])
    ETH_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[0])
    ETH_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[0])
    # POLY
    POLY_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[1])
    POLY_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[1])
    POLY_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[1])
    # Moonriver
    MOONRIVER_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[2])
    MOONRIVER_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[2])
    MOONRIVER_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[2])
    # Moonbeam
    MOONBEAM_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[3])
    MOONBEAM_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[3])
    MOONBEAM_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[3])
    # Bscscan
    BSCSCAN_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[4])
    BSCSCAN_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[4])
    BSCSCAN_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[4])
    # Avalanchescan
    AVALANCHE_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[5])
    AVALANCHE_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[5])
    AVALANCHE_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[5])
    # Fantomscan
    FTM_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[6])
    FTM_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[6])
    FTM_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[6])
    # optimism
    OP_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[7])
    OP_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[7])
    # arbitrum
    AR_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[8])
    # Kavascan
    KAVA_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[9])
    KAVA_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[9])
    KAVA_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[9])
    # Dogechain
    DOGE_USDT = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[0], n_explorer[10])
    DOGE_USDC = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[1], n_explorer[10])
    DOGE_BUSD = Filter_data_duplicateValue(
        DF_MULTICHAIN, n_label[2], n_explorer[10])

    # ETH SUM
    Frames_eth = [ETH_USDT, ETH_USDC, ETH_BUSD]
    ETH_S = pd.concat(Frames_eth)
    # Poly
    Frames_poly = [POLY_USDT, POLY_USDC, POLY_BUSD]
    POLY_S = pd.concat(Frames_poly)
    POLY_S
    # Moonriver
    Frams_Moonriver = [MOONRIVER_USDT, MOONRIVER_USDC, MOONRIVER_BUSD]
    MOONRI_S = pd.concat(Frams_Moonriver)
    # Moonbeam
    Frames_Moonbeam = [MOONBEAM_USDT, MOONBEAM_USDC, MOONBEAM_BUSD]
    MOONBEAM_S = pd.concat(Frames_Moonbeam)
    # Bscscan
    Frames_bscscan = [BSCSCAN_USDT, BSCSCAN_USDC, BSCSCAN_BUSD]
    BSCSCAN_S = pd.concat(Frames_bscscan)

    # Avalanche
    Frames_avalanche = [AVALANCHE_USDT, AVALANCHE_USDC, AVALANCHE_BUSD]
    AVALAN_S = pd.concat(Frames_avalanche)
    # FTMScan
    Frames_FTM = [FTM_USDT, FTM_USDC, FTM_BUSD]
    FTM_S = pd.concat(Frames_FTM)
    # OPtimism
    Frames_OPti = [OP_USDT, OP_USDC]
    OPTI_S = pd.concat(Frames_OPti)
    # Arbiscan
    AR_USDT

    # Kavascan
    Frames_kava = [KAVA_USDT, KAVA_USDC, KAVA_BUSD]
    KAVA_S = pd.concat(Frames_kava)

    # Dogechain
    Frames_Doge = [DOGE_USDT, DOGE_USDC, DOGE_BUSD]
    DOGE_S = pd.concat(Frames_Doge)

    Frames_all = [ETH_S, POLY_S, MOONRI_S, MOONBEAM_S, BSCSCAN_S,
                  AVALAN_S, FTM_S, OPTI_S, AR_USDT, KAVA_S, DOGE_S]
    DF_MULTICHAIN_V1 = pd.concat(Frames_all)
    return DF_MULTICHAIN_V1


def proces_Data_Multichain():
    DATA = processing_Data(DF_MULTICHAIN, n_label, n_explorer)
    return DATA

# Bridge_Mul = proces_Data_Multichain()

def TOTAL_ASSETS(data, explorer):
    ''' 
    FUNTION: Summary all value stabelcoin the same explorer and transfer another dataframe
    '''
    tmp = data[data['EXPLORER'] == explorer]
    tmp = tmp.groupby(['TIMESTAMP', 'LABEL'])['VALUE'].sum().reset_index()
    tmp = tmp.groupby(['TIMESTAMP'])['VALUE'].sum().reset_index()
    tmp['EXPLORER'] = explorer
    return tmp
def rename(data):
    data['EXPLORER'] = data['EXPLORER'].replace({'Etherscan':'Ethereum','Polygonscan':'Polygon','Bscscan':'BSC','Avalanchescan':'Avalanche','Fantomscan':'Fantom','Kavascan':'Kava'})
    return data


Bridge_line = proces_Data_Multichain()
Bridge_line = rename(Bridge_line)
Etherscan = TOTAL_ASSETS(Bridge_line,'Ethereum')
Polygonscan = TOTAL_ASSETS(Bridge_line,'Polygon')
Moonriver = TOTAL_ASSETS(Bridge_line,'Moonriver')
Moonbeam = TOTAL_ASSETS(Bridge_line,'Moonbeam')
Bscscan = TOTAL_ASSETS(Bridge_line,'BSC')
Avalanchescan = TOTAL_ASSETS(Bridge_line,'Avalanche')
Fantomscan = TOTAL_ASSETS(Bridge_line,'Fantom')
Optimsm = TOTAL_ASSETS(Bridge_line,'Optimsm')
Arbitrum = TOTAL_ASSETS(Bridge_line,'Arbitrum')
Kavascan = TOTAL_ASSETS(Bridge_line,'Kava')
Dogechain = TOTAL_ASSETS(Bridge_line,'Dogechain')
TOTAL_MULTICHAIN = pd.concat([Etherscan,Polygonscan,Moonriver,Moonbeam,Bscscan,Avalanchescan,Fantomscan,Optimsm,Arbitrum,Kavascan,Dogechain])
def create_multichain(data):
    cols = ['TIMESTAMP','VALUE','EXPLORER']
    data = data[cols]
    data =data.groupby(['TIMESTAMP','EXPLORER'])['VALUE'].sum()
    data = data.reset_index()
    # data = data.sort_values(by=['VALUE'],ascending=False)
    return data

# eposo


# Celer cBridge
query_celer_cbridge = os.environ['query_celer_cBridge']

Celer_cBridge = pd.read_sql(query_celer_cbridge,my_server)

Celer_cBridge['TIMESTAMP']=Celer_cBridge['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
Celer_cBridge['TIMESTAMP'] = pd.to_datetime(Celer_cBridge['TIMESTAMP'])

Celer_cBridge['EXPLORER'] = Celer_cBridge['EXPLORER'].replace({'Polygonscan':'Polygon','Bscscan':'BSC','Avalanchescan':'Avalanche','Fantomscan':'Fantom'})
#line total assets
def create_celer(data):
    TOTAL_ASSETS_CELER =data.groupby(['TIMESTAMP','EXPLORER'])['VALUE'].sum()
    TOTAL_ASSETS_CELER = TOTAL_ASSETS_CELER.reset_index()
    # TOTAL_ASSETS_CELER = TOTAL_ASSETS_CELER.sort_values(by=['VALUE'])
    return TOTAL_ASSETS_CELER


# Hop bridge
query_hop_bridge = os.environ['query_hop_bridge']
HOP = pd.read_sql(query_hop_bridge,my_server)

HOP['TIMESTAMP']=HOP['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
HOP['TIMESTAMP'] = pd.to_datetime(HOP['TIMESTAMP'])


#Line total
def create_hop(data):
    cols = ['TIMESTAMP','VALUE','EXPLORER']
    TOTAL_ASSETS_HOP = data.groupby(['TIMESTAMP','EXPLORER'])['VALUE'].sum()
    TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP.reset_index()
    # TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP.sort_values(by=['VALUE'])
    TOTAL_ASSETS_HOP = TOTAL_ASSETS_HOP[cols]
    TOTAL_ASSETS_HOP= rename(TOTAL_ASSETS_HOP)
    return TOTAL_ASSETS_HOP


# Startgate
query_stargate = os.environ['query_Stargate_bridge']

STARGATE = pd.read_sql(query_stargate,my_server)
STARGATE = STARGATE.rename(columns={'TIMSTAMP':'TIMESTAMP'})

STARGATE['TIMESTAMP']=STARGATE['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
STARGATE['TIMESTAMP'] = pd.to_datetime(STARGATE['TIMESTAMP'])

#line total assets
def create_starage(data):
    cols = ['TIMESTAMP','VALUE','EXPLORER']
    TOTAL_ASSETS_STARGATE =data.groupby(['TIMESTAMP','EXPLORER'])['VALUE'].sum()
    TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE.reset_index()
    # TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE.sort_values(by=['VALUE'])
    TOTAL_ASSETS_STARGATE = TOTAL_ASSETS_STARGATE[cols]
    TOTAL_ASSETS_STARGATE = rename(TOTAL_ASSETS_STARGATE)
    return TOTAL_ASSETS_STARGATE

#Synapse
query_synapse = os.getenv('query_synapse_bridge')

SYNAPSE = pd.read_sql(query_synapse,my_server)
SYNAPSE['TIMESTAMP']=SYNAPSE['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
SYNAPSE['TIMESTAMP'] = pd.to_datetime(SYNAPSE['TIMESTAMP'])

def create_synapse(data):

    cols = ['TIMESTAMP','VALUE','EXPLORER']

    TOTAL_ASSETS_SYNAPSE =SYNAPSE.groupby(['TIMESTAMP','EXPLORER'])['VALUE'].sum()
    TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE.reset_index()
    # TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE.sort_values(by=['VALUE'])
    TOTAL_ASSETS_SYNAPSE = TOTAL_ASSETS_SYNAPSE[cols]
    TOTAL_ASSETS_SYNAPSE = rename(TOTAL_ASSETS_SYNAPSE)
    return TOTAL_ASSETS_SYNAPSE


def create_bridge_pie(multichain,celer,hop,stargate,synapse,label:str):
    cols = ['EXPLORER','VALUE']
    multichain = multichain[multichain['TIMESTAMP']==multichain['TIMESTAMP'].max()]
    celer = celer[celer['TIMESTAMP']==celer['TIMESTAMP'].max()]
    hop = hop[hop['TIMESTAMP']==hop['TIMESTAMP'].max()]
    stargate = stargate[stargate['TIMESTAMP']==stargate['TIMESTAMP'].max()]
    synapse = synapse[synapse['TIMESTAMP']==synapse['TIMESTAMP'].max()]
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if label not in choice_condition:
        return f'label: {label} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
    elif label=='Multichain':
        multichain = multichain[cols]
        return multichain
    elif label =='Celer':
        celer = celer[cols]
        return celer
    elif label=="Hop":
        hop = hop[cols]
        return hop
    elif label=='Stargate':
        stargate = stargate[cols]
        return stargate
    elif label=='Synapse':
        synapse = synapse[cols]
        return synapse
    
multichain_pie = create_multichain(Bridge_line)
multichain_pie = multichain_pie.reset_index()
multichain_pie = multichain_pie[multichain_pie['TIMESTAMP']==multichain_pie['TIMESTAMP'].max()]
multichain_pie = multichain_pie.sort_values(by=['VALUE'],ascending=False)


celer_pie = create_celer(Celer_cBridge)
celer_pie= celer_pie.reset_index()
celer_pie = celer_pie[celer_pie['TIMESTAMP']==celer_pie['TIMESTAMP'].max()]
celer_pie = celer_pie.sort_values(by=['VALUE'],ascending=False)

hop_pie = create_hop(HOP).reset_index()
hop_pie = hop_pie[hop_pie['TIMESTAMP']==hop_pie['TIMESTAMP'].max()]
hop_pie = hop_pie.sort_values(by=['VALUE'],ascending=False)
stargate_pie = create_starage(STARGATE).reset_index()
stargate_pie = stargate_pie[stargate_pie['TIMESTAMP']==stargate_pie['TIMESTAMP'].max()]
stargate_pie = stargate_pie.sort_values(by=['VALUE'],ascending=False)
synapse_pie = create_synapse(SYNAPSE).reset_index()
synapse_pie = synapse_pie[synapse_pie['TIMESTAMP']==synapse_pie['TIMESTAMP'].max()]
synapse_pie = synapse_pie.sort_values(by=['VALUE'],ascending=False)

#test_show all in line chart Bridge

# table in header ovewrview
multichain_table = create_multichain(TOTAL_MULTICHAIN)
multichain_table = multichain_table.groupby(['TIMESTAMP']).agg({'VALUE':'sum'}).reset_index()

celer_table = create_celer(Celer_cBridge)
celer_table = celer_table.groupby(['TIMESTAMP']).agg({'VALUE':'sum'}).reset_index()
hop_table = create_hop(HOP)
hop_table = hop_table.groupby(['TIMESTAMP']).agg({'VALUE':'sum'}).reset_index()
stargate_table = create_starage(STARGATE)
stargate_table = stargate_table.groupby(['TIMESTAMP']).agg({'VALUE':'sum'}).reset_index()
synapse_table = create_synapse(SYNAPSE)
synapse_table = synapse_table.groupby(['TIMESTAMP']).agg({'VALUE':'sum'}).reset_index()

def create_table_bridge_st(data):
    hientai = data[data['TIMESTAMP']== data['TIMESTAMP'].max()]['VALUE']
    # hientai = hientai.groupby(['TIMESTAMP']).agg({'VALUE':'sum'}).reset_index()['VALUE']
    qk_data = data.set_index('TIMESTAMP')
    qk_data = qk_data.between_time('6:00', '10:59')
    qk_data = qk_data.reset_index()
    qk_data['TIMESTAMP'] = pd.to_datetime(qk_data['TIMESTAMP']).dt.date

    lastday = qk_data[qk_data['TIMESTAMP']== qk_data['TIMESTAMP'].max() - datetime.timedelta(days=1)]['VALUE']
    lastweek = qk_data[qk_data['TIMESTAMP']== qk_data['TIMESTAMP'].max() - datetime.timedelta(days=7)]['VALUE']
    lastmonth = qk_data[qk_data['TIMESTAMP']== qk_data['TIMESTAMP'].max() - datetime.timedelta(days=30)]['VALUE']
    df_table = pd.DataFrame({
        'changeVL_24h':[float(hientai)-float(lastday)],
        'per_24h':[((float(hientai)-float(lastday))/float(hientai))*100],
        'changeVL_7d':[float(hientai)-float(lastweek)],
        'per_7D':[((float(hientai)-float(lastweek))/float(hientai))*100],
        'change_30d':[float(hientai)-float(lastmonth)],
        'per_30D':[((float(hientai)-float(lastmonth))/float(hientai))*100],
    })
    return df_table.to_dict(orient='records')


# Header thay đổi tuừng phàn tử trong Bridge

multichain_satis = Bridge_line.groupby(['TIMESTAMP','LABEL']).agg({'VALUE':'sum'}).reset_index()
celer_satis = Celer_cBridge.groupby(['TIMESTAMP','LABEL']).agg({'VALUE':'sum'}).reset_index()
hop_statis = HOP.groupby(['TIMESTAMP','LABEL']).agg({'VALUE':'sum'}).reset_index()
stargate_static = STARGATE.groupby(['TIMESTAMP','LABEL']).agg({'VALUE':'sum'}).reset_index()
synapse_static =SYNAPSE.groupby(['TIMESTAMP','LABEL']).agg({'VALUE':'sum'}).reset_index()

def choice_df_statics(bridge:str,token:str):
    choice_token = ['USDT','USDC','BUSD']
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if bridge not in choice_condition and token not in choice_token:
        return f'label: {bridge} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]\n token: {token} is not found choice ["USDT","USDC","BUSD"]'
    elif bridge=='Multichain' and token=='USDT':
        data = multichain_satis[multichain_satis['LABEL']=='USDT']
        return data
    elif bridge=='Multichain' and token=='USDC':
        data = multichain_satis[multichain_satis['LABEL']=='USDC']
        return data
    elif bridge=='Multichain' and token=='BUSD':
        data = multichain_satis[multichain_satis['LABEL']=='BUSD']
        return data
    # celer
    elif bridge=='Celer' and token=='USDT':
        data = celer_satis[celer_satis['LABEL']=='USDT']
        return data
    elif bridge=='Celer' and token=='USDC':
        data = celer_satis[celer_satis['LABEL']=='USDC']
        return data
    elif bridge=='Celer' and token=='BUSD':
        data = celer_satis[celer_satis['LABEL']=='BUSD']
        return data
    #hop
    elif bridge=='Hop' and token=='USDT':
        data = hop_statis[hop_statis['LABEL']=='USDT']
        return data
    elif bridge=='Hop' and token=='USDC':
        data = hop_statis[hop_statis['LABEL']=='USDC']
        return data
    elif bridge=='Hop' and token=='BUSD':
        data = hop_statis[hop_statis['LABEL']=='BUSD']
        return data
    #stargate
    elif bridge=='Stargate' and token=='USDT':
        data = stargate_static[stargate_static['LABEL']=='USDT']
        return data
    elif bridge=='Stargate' and token=='USDC':
        data = stargate_static[stargate_static['LABEL']=='USDC']
        return data
    elif bridge=='Stargate' and token=='BUSD':
        data = stargate_static[stargate_static['LABEL']=='BUSD']
        return data
    #synapse
    elif bridge=='Synapse' and token=='USDT':
        data = synapse_static[synapse_static['LABEL']=='USDT']
        return data
    elif bridge=='Synapse' and token=='USDC':
        data = synapse_static[synapse_static['LABEL']=='USDC']
        return data
    elif bridge=='Synapse' and token=='BUSD':
        data = synapse_static[synapse_static['LABEL']=='BUSD']
        return data
def create_table_statis_eachofbridge(data):
    hientai = data[data['TIMESTAMP']== data['TIMESTAMP'].max()]['VALUE']
    qk_data = data.set_index('TIMESTAMP')
    qk_data = qk_data.between_time('6:00', '10:59')
    qk_data = qk_data.reset_index()
    qk_data['TIMESTAMP'] = pd.to_datetime(qk_data['TIMESTAMP']).dt.date
    lastday = qk_data[qk_data['TIMESTAMP']== qk_data['TIMESTAMP'].max() - datetime.timedelta(days=1)]['VALUE']
    lastweek = qk_data[qk_data['TIMESTAMP']== qk_data['TIMESTAMP'].max() - datetime.timedelta(days=7)]['VALUE']
    lastmonth = qk_data[qk_data['TIMESTAMP']== qk_data['TIMESTAMP'].max() - datetime.timedelta(days=30)]['VALUE']
    df_table = pd.DataFrame({
        'changeVL_24h':[float(hientai)-float(lastday)],
        'per_24h':[((float(hientai)-float(lastday))/float(hientai))*100],
        'changeVL_7d':[float(hientai)-float(lastweek)],
        'per_7D':[((float(hientai)-float(lastweek))/float(hientai))*100],
        'change_30d':[float(hientai)-float(lastmonth)],
        'per_30D':[((float(hientai)-float(lastmonth))/float(hientai))*100],
    })
    return df_table.to_dict(orient='records')