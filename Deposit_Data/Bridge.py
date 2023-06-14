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
def rename(data):
    data['Name'] = data['Name'].replace({'Etherscan':'Ethereum','Polygonscan':'Polygon','Bscscan':'BSC','Avalanchescan':'Avalanche','Fantomscan':'Fantom','Kavascan':'Kava'})
    return data
Deposit_mul = proces_Data_Multichain()

# Deposit
def create_Deposit_multichain(Deposit_mul):
    ETH_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Etherscan']
    ETH_MUl = ETH_MUl.reset_index()

    ETH_MUl = pd.DataFrame(ETH_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    ETH_MUl['VALUE_QK'] = ETH_MUl['VALUE'].shift(1).fillna(0)
    ETH_MUL = ETH_MUl.iloc[1:]
    ETH_MUL['Value'] = ETH_MUL['VALUE'] - ETH_MUL['VALUE_QK']
    ETH_MUL['Name'] = 'Ethereum'


    #Polygonscan
    Deposit_mul['EXPLORER'].unique()
    Polygonscan_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Polygonscan']
    Polygonscan_MUl = Polygonscan_MUl.reset_index()
    Polygonscan_MUl = pd.DataFrame(Polygonscan_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Polygonscan_MUl['VALUE_QK'] = Polygonscan_MUl['VALUE'].shift(1).fillna(0)
    Polygonscan_MUl = Polygonscan_MUl.iloc[1:]
    Polygonscan_MUl['Value'] = Polygonscan_MUl['VALUE'] - Polygonscan_MUl['VALUE_QK']

    Polygonscan_MUl['Name'] = 'Polygon'
    #Moonriver
    Deposit_mul['EXPLORER'].unique()
    Moonriver_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Moonriver']
    Moonriver_MUl = Moonriver_MUl.reset_index()
    Moonriver_MUl = pd.DataFrame(Moonriver_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Moonriver_MUl['VALUE_QK'] = Moonriver_MUl['VALUE'].shift(1).fillna(0)
    Moonriver_MUl = Moonriver_MUl.iloc[1:]
    Moonriver_MUl['Value'] = Moonriver_MUl['VALUE'] - Moonriver_MUl['VALUE_QK']

    Moonriver_MUl['Name'] = 'Moonriver'

    #Moonbeam
    Deposit_mul['EXPLORER'].unique()
    Moonbeam_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Moonbeam']
    Moonbeam_MUl = Moonbeam_MUl.reset_index()
    Moonbeam_MUl = pd.DataFrame(Moonbeam_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Moonbeam_MUl['VALUE_QK'] = Moonbeam_MUl['VALUE'].shift(1).fillna(0)
    Moonbeam_MUl = Moonbeam_MUl.iloc[1:]
    Moonbeam_MUl['Value'] = Moonbeam_MUl['VALUE'] - Moonbeam_MUl['VALUE_QK']

    Moonbeam_MUl['Name'] = 'Moonbeam'

    #Bscscan
    Deposit_mul['EXPLORER'].unique()
    Bscscan_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Bscscan']
    Bscscan_MUl = Bscscan_MUl.reset_index()
    Bscscan_MUl = pd.DataFrame(Bscscan_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Bscscan_MUl['VALUE_QK'] = Bscscan_MUl['VALUE'].shift(1).fillna(0)
    Bscscan_MUl = Bscscan_MUl.iloc[1:]
    Bscscan_MUl['Value'] = Bscscan_MUl['VALUE'] - Bscscan_MUl['VALUE_QK']

    Bscscan_MUl['Name'] = 'BSC'

    #Avalanchescan
    Deposit_mul['EXPLORER'].unique()
    Avalanchescan_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Avalanchescan']
    Avalanchescan_MUl = Avalanchescan_MUl.reset_index()
    Avalanchescan_MUl = pd.DataFrame(Avalanchescan_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Avalanchescan_MUl['VALUE_QK'] = Avalanchescan_MUl['VALUE'].shift(1).fillna(0)
    Avalanchescan_MUl = Avalanchescan_MUl.iloc[1:]
    Avalanchescan_MUl['Value'] = Avalanchescan_MUl['VALUE'] - Avalanchescan_MUl['VALUE_QK']

    Avalanchescan_MUl['Name'] = 'Avalanche'

    #Fantomscan
    Deposit_mul['EXPLORER'].unique()
    Fantomscan_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Fantomscan']
    Fantomscan_MUl = Fantomscan_MUl.reset_index()
    Fantomscan_MUl = pd.DataFrame(Fantomscan_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Fantomscan_MUl['VALUE_QK'] = Fantomscan_MUl['VALUE'].shift(1).fillna(0)
    Fantomscan_MUl = Fantomscan_MUl.iloc[1:]
    Fantomscan_MUl['Value'] = Fantomscan_MUl['VALUE'] - Fantomscan_MUl['VALUE_QK']

    Fantomscan_MUl['Name'] = 'Fantom'

    #Optimsm
    Deposit_mul['EXPLORER'].unique()
    Optimsm_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Optimsm']
    Optimsm_MUl = Optimsm_MUl.reset_index()
    Optimsm_MUl = pd.DataFrame(Optimsm_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Optimsm_MUl['VALUE_QK'] = Optimsm_MUl['VALUE'].shift(1).fillna(0)
    Optimsm_MUl = Optimsm_MUl.iloc[1:]
    Optimsm_MUl['Value'] = Optimsm_MUl['VALUE'] - Optimsm_MUl['VALUE_QK']

    Optimsm_MUl['Name'] = 'Optimsm'

    #Arbitrum
    Deposit_mul['EXPLORER'].unique()
    Arbitrum_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Arbitrum']
    Arbitrum_MUl = Arbitrum_MUl.reset_index()
    Arbitrum_MUl = pd.DataFrame(Arbitrum_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Arbitrum_MUl['VALUE_QK'] = Arbitrum_MUl['VALUE'].shift(1).fillna(0)
    Arbitrum_MUl = Arbitrum_MUl.iloc[1:]
    Arbitrum_MUl['Value'] = Arbitrum_MUl['VALUE'] - Arbitrum_MUl['VALUE_QK']

    Arbitrum_MUl['Name'] = 'Arbitrum'

    #Kavascan
    Deposit_mul['EXPLORER'].unique()
    Kavascan_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Kavascan']
    Kavascan_MUl = Kavascan_MUl.reset_index()
    Kavascan_MUl = pd.DataFrame(Kavascan_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Kavascan_MUl['VALUE_QK'] = Kavascan_MUl['VALUE'].shift(1).fillna(0)
    Kavascan_MUl = Kavascan_MUl.iloc[1:]
    Kavascan_MUl['Value'] = Kavascan_MUl['VALUE'] - Kavascan_MUl['VALUE_QK']

    Kavascan_MUl['Name'] = 'Kava'


    #Dogechain
    Deposit_mul['EXPLORER'].unique()
    Dogechain_MUl = Deposit_mul[Deposit_mul['EXPLORER']=='Dogechain']
    Dogechain_MUl = Dogechain_MUl.reset_index()
    Dogechain_MUl = pd.DataFrame(Dogechain_MUl.groupby(['TIMESTAMP'])['VALUE'].sum())
    Dogechain_MUl['VALUE_QK'] = Dogechain_MUl['VALUE'].shift(1).fillna(0)
    Dogechain_MUl = Dogechain_MUl.iloc[1:]
    Dogechain_MUl['Value'] = Dogechain_MUl['VALUE'] - Dogechain_MUl['VALUE_QK']
    Dogechain_MUl['Name'] = 'Dogechain'
    concat_df_mul = pd.concat([ETH_MUL,Polygonscan_MUl,Moonbeam_MUl,Moonriver_MUl,Bscscan_MUl,Avalanchescan_MUl,Fantomscan_MUl,Optimsm_MUl,Arbitrum_MUl,Kavascan_MUl,Dogechain_MUl])

    concat_df_mul = concat_df_mul.reset_index()
    concat_df_mul['TIMESTAMP'] = concat_df_mul['TIMESTAMP'].apply(pd.to_datetime)
    cols =['TIMESTAMP','Value','Name']
    concat_df_mul = concat_df_mul[cols]
    return concat_df_mul
# print(create_Deposit_multichain(Deposit_mul))
# HOP

query_hop_bridge = os.environ['query_hop_bridge']
Deposit_hop = pd.read_sql(query_hop_bridge, my_server)

Deposit_hop['TIMESTAMP'] = Deposit_hop['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
Deposit_hop['TIMESTAMP'] = pd.to_datetime(Deposit_hop['TIMESTAMP'])

def create_df_deposit_hop(Deposit_hop):
    # POLYGON
    Polygonscan = Deposit_hop[Deposit_hop['EXPLORER']
                                == 'Polygonscan'].reset_index()
    POL_HOP = pd.DataFrame(Polygonscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    POL_HOP['VALUE_QK'] = POL_HOP['VALUE'].shift(1).fillna(0)
    POL_HOP = POL_HOP.iloc[1:]
    POL_HOP['Value'] = round(POL_HOP['VALUE'] - POL_HOP['VALUE_QK'], 2)
    POL_HOP['Name'] = 'Polygon'
    # Arbitrum
    Arbitrum = Deposit_hop[Deposit_hop['EXPLORER'] == 'Arbitrum'].reset_index()
    ARB_HOP = pd.DataFrame(Arbitrum.groupby(['TIMESTAMP'])['VALUE'].sum())
    ARB_HOP['VALUE_QK'] = ARB_HOP['VALUE'].shift(1).fillna(0)
    ARB_HOP = ARB_HOP.iloc[1:]
    ARB_HOP['Value'] = round(ARB_HOP['VALUE'] - ARB_HOP['VALUE_QK'], 2)
    ARB_HOP['Name'] = 'Arbitrum'
    # Optimsm
    Optimsm = Deposit_hop[Deposit_hop['EXPLORER'] == 'Optimsm'].reset_index()
    OPT_HOP = pd.DataFrame(Optimsm.groupby(['TIMESTAMP'])['VALUE'].sum())
    OPT_HOP['VALUE_QK'] = OPT_HOP['VALUE'].shift(1).fillna(0)
    OPT_HOP = OPT_HOP.iloc[1:]
    OPT_HOP['Value'] = round(OPT_HOP['VALUE'] - OPT_HOP['VALUE_QK'], 2)
    OPT_HOP['Name'] = 'Optimsm'

    concat_df_hop = pd.concat([POL_HOP, ARB_HOP, OPT_HOP], axis=0)

    concat_df_hop = concat_df_hop.reset_index()
    concat_df_hop['TIME_DATE'] = concat_df_hop['TIMESTAMP'].dt.date

    concat_df_hop['TIMESTAMP'] = concat_df_hop["TIMESTAMP"].apply(pd.to_datetime)
    cols =['TIMESTAMP','Value','Name']
    concat_df_hop = concat_df_hop[cols]
    return concat_df_hop

# celer
query_celer_cbridge = os.environ['query_celer_cBridge']

Celer_cBridge = pd.read_sql(query_celer_cbridge,my_server)
Celer_cBridge['TIMESTAMP']=Celer_cBridge['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
Celer_cBridge['TIMESTAMP'] = pd.to_datetime(Celer_cBridge['TIMESTAMP'])
Celer_cBridge =Celer_cBridge.groupby(['TIMESTAMP','LABEL','EXPLORER'])['VALUE'].sum().reset_index()
def create_df_deposit_celer(Celer_cBridge):
    Arbitrum = Celer_cBridge[Celer_cBridge['EXPLORER'] == 'Arbitrum'].reset_index()
    Arbitrum = pd.DataFrame(Arbitrum.groupby(['TIMESTAMP'])['VALUE'].sum())
    Arbitrum['VALUE_QK'] = Arbitrum['VALUE'].shift(1).fillna(0)
    Arbitrum = Arbitrum.iloc[1:]
    Arbitrum['Value'] = Arbitrum['VALUE'] - Arbitrum['VALUE_QK']
    Arbitrum['Name'] = 'Arbitrum'

    #Avalanchescan
    Avalanchescan = Celer_cBridge[Celer_cBridge['EXPLORER']=='Avalanchescan'].reset_index()
    Avalanchescan = pd.DataFrame(Avalanchescan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Avalanchescan['VALUE_QK'] = Avalanchescan['VALUE'].shift(1).fillna(0)
    Avalanchescan = Avalanchescan.iloc[1:]
    Avalanchescan['Value'] = Avalanchescan['VALUE'] - Avalanchescan['VALUE_QK']
    Avalanchescan['Name'] = 'Avalanche'
    #Bscscan
    Bscscan = Celer_cBridge[Celer_cBridge['EXPLORER']=='Bscscan'].reset_index()
    Bscscan = pd.DataFrame(Bscscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Bscscan['VALUE_QK'] = Bscscan['VALUE'].shift(1).fillna(0)
    Bscscan = Bscscan.iloc[1:]
    Bscscan['Value'] = Bscscan['VALUE'] - Bscscan['VALUE_QK']
    Bscscan['Name']='Bscscan'
    #Fantomscan

    Fantomscan = Celer_cBridge[Celer_cBridge['EXPLORER']=='Fantomscan'].reset_index()
    Fantomscan = pd.DataFrame(Fantomscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Fantomscan['VALUE_QK'] = Fantomscan['VALUE'].shift(1).fillna(0)
    Fantomscan = Fantomscan.iloc[1:]
    Fantomscan['Value'] = Fantomscan['VALUE'] - Fantomscan['VALUE_QK']
    Fantomscan['Name'] = 'Fantom'
    #Optimsm

    Optimsm = Celer_cBridge[Celer_cBridge['EXPLORER']=='Optimsm'].reset_index()
    Optimsm = pd.DataFrame(Optimsm.groupby(['TIMESTAMP'])['VALUE'].sum())
    Optimsm['VALUE_QK'] = Optimsm['VALUE'].shift(1).fillna(0)
    Optimsm = Optimsm.iloc[1:]
    Optimsm['Value'] = Optimsm['VALUE'] - Optimsm['VALUE_QK']
    Optimsm['Name'] = 'Optimsm'
    #Polygonscan
    Polygonscan = Celer_cBridge[Celer_cBridge['EXPLORER']=='Polygonscan'].reset_index()
    Polygonscan = pd.DataFrame(Polygonscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Polygonscan['VALUE_QK'] = Polygonscan['VALUE'].shift(1).fillna(0)
    Polygonscan = Polygonscan.iloc[1:]
    Polygonscan['Value'] = Polygonscan['VALUE'] - Polygonscan['VALUE_QK']
    Polygonscan['Name'] = 'Polygonscan'

    concat_df_celer =pd.concat([Arbitrum,Avalanchescan,Bscscan,Fantomscan,Optimsm,Polygonscan],axis=0).reset_index()
    cols =['TIMESTAMP','Value','Name']
    concat_df_celer = concat_df_celer[cols]
    return concat_df_celer

# Stargate
query_stargate = os.environ['query_Stargate_bridge']

STARGATE = pd.read_sql(query_stargate,my_server)
STARGATE = STARGATE.rename(columns={'TIMSTAMP':'TIMESTAMP'})

STARGATE['TIMESTAMP']=STARGATE['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
STARGATE['TIMESTAMP'] = pd.to_datetime(STARGATE['TIMESTAMP'])
def create_df_deposit_stargate(STARGATE):
    #Bscscan
    Bscscan = STARGATE[STARGATE['EXPLORER']=='Bscscan'].reset_index()
    Bscscan = pd.DataFrame(Bscscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Bscscan['VALUE_QK'] = Bscscan['VALUE'].shift(1).fillna(0)
    Bscscan = Bscscan.iloc[1:]
    Bscscan['Value'] = Bscscan['VALUE'] - Bscscan['VALUE_QK']
    Bscscan['Name'] = 'BSC'

    #Arbitrum
    Arbitrum = STARGATE[STARGATE['EXPLORER']=='Arbitrum'].reset_index()
    Arbitrum = pd.DataFrame(Arbitrum.groupby(['TIMESTAMP'])['VALUE'].sum())
    Arbitrum['VALUE_QK'] = Arbitrum['VALUE'].shift(1).fillna(0)
    Arbitrum = Arbitrum.iloc[1:]
    Arbitrum['Value'] = Arbitrum['VALUE'] - Arbitrum['VALUE_QK']
    Arbitrum['Name'] = 'Arbitrum'

    #Avalanchescan
    Avalanchescan = STARGATE[STARGATE['EXPLORER']=='Avalanchescan'].reset_index()
    Avalanchescan = pd.DataFrame(Avalanchescan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Avalanchescan['VALUE_QK'] = Avalanchescan['VALUE'].shift(1).fillna(0)
    Avalanchescan = Avalanchescan.iloc[1:]
    Avalanchescan['Value'] = Avalanchescan['VALUE'] - Avalanchescan['VALUE_QK']
    Avalanchescan['Name'] = 'Avalanche'

    #Etherscan

    Etherscan = STARGATE[STARGATE['EXPLORER']=='Etherscan'].reset_index()
    Etherscan = pd.DataFrame(Etherscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Etherscan['VALUE_QK'] = Etherscan['VALUE'].shift(1).fillna(0)
    Etherscan = Etherscan.iloc[1:]
    Etherscan['Value'] = Etherscan['VALUE'] - Etherscan['VALUE_QK']
    Etherscan['Name'] = 'Ethereum'

    #Fantomscan

    Fantomscan = STARGATE[STARGATE['EXPLORER']=='Fantomscan'].reset_index()
    Fantomscan = pd.DataFrame(Fantomscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Fantomscan['VALUE_QK'] = Fantomscan['VALUE'].shift(1).fillna(0)
    Fantomscan = Fantomscan.iloc[1:]
    Fantomscan['Value'] = Fantomscan['VALUE'] - Fantomscan['VALUE_QK']
    Fantomscan['Name'] = 'Fantom'

    #Optimsm

    Optimsm = STARGATE[STARGATE['EXPLORER']=='Optimsm'].reset_index()
    Optimsm = pd.DataFrame(Optimsm.groupby(['TIMESTAMP'])['VALUE'].sum())
    Optimsm['VALUE_QK'] = Optimsm['VALUE'].shift(1).fillna(0)
    Optimsm = Optimsm.iloc[1:]
    Optimsm['Value'] = Optimsm['VALUE'] - Optimsm['VALUE_QK']
    Optimsm['Name'] = 'Optimsm'

    #Polygonscan
    Polygonscan = STARGATE[STARGATE['EXPLORER']=='Polygonscan'].reset_index()
    Polygonscan = pd.DataFrame(Polygonscan.groupby(['TIMESTAMP'])['VALUE'].sum())
    Polygonscan['VALUE_QK'] = Polygonscan['VALUE'].shift(1).fillna(0)
    Polygonscan = Polygonscan.iloc[1:]
    Polygonscan['Value'] = Polygonscan['VALUE'] - Polygonscan['VALUE_QK']
    Polygonscan['Name'] = 'Polygon'

    #Metis
    Metis = STARGATE[STARGATE['EXPLORER']=='Metis'].reset_index()
    Metis = pd.DataFrame(Metis.groupby(['TIMESTAMP'])['VALUE'].sum())
    Metis['VALUE_QK'] = Metis['VALUE'].shift(1).fillna(0)
    Metis = Metis.iloc[1:]
    Metis['Value'] = Metis['VALUE'] - Metis['VALUE_QK']
    Metis['Name'] = 'Metis'

    concat_df_stargate = pd.concat([Bscscan,Arbitrum,Avalanchescan,Etherscan,Fantomscan,Optimsm,Polygonscan,Metis],axis=0).reset_index()
    cols =['TIMESTAMP','Value','Name']
    concat_df_stargate = concat_df_stargate[cols]
    return concat_df_stargate

#Synapse

query_synapse = os.getenv('query_synapse_bridge')

SYNAPSE = pd.read_sql(query_synapse,my_server)
SYNAPSE['TIMESTAMP']=SYNAPSE['TIMESTAMP'].apply(lambda x : pd.to_datetime(x).floor('T'))
SYNAPSE['TIMESTAMP'] = pd.to_datetime(SYNAPSE['TIMESTAMP'])
print(SYNAPSE['EXPLORER'].unique())

def create_df_deposit_synapse(SYNAPSE):
    SYNAPSE_ETH = SYNAPSE[SYNAPSE['EXPLORER']=='Ethereum']
    SYNAPSE_ETH = SYNAPSE_ETH.reset_index()

    SYNAPSE_ETH = pd.DataFrame(SYNAPSE_ETH.groupby(['TIMESTAMP'])['VALUE'].sum())
    SYNAPSE_ETH['VALUE_QK'] = SYNAPSE_ETH['VALUE'].shift(1).fillna(0)
    SYNAPSE_ETH = SYNAPSE_ETH.iloc[1:]
    SYNAPSE_ETH['Value'] = SYNAPSE_ETH['VALUE'] - SYNAPSE_ETH['VALUE_QK']
    SYNAPSE_ETH['Name'] = 'Ethereum'

    #polygon
    SYNAPSE_POL = SYNAPSE[SYNAPSE['EXPLORER']=='Polygon']
    SYNAPSE_POL = SYNAPSE_POL.reset_index()

    SYNAPSE_POL = pd.DataFrame(SYNAPSE_POL.groupby(['TIMESTAMP'])['VALUE'].sum())
    SYNAPSE_POL['VALUE_QK'] = SYNAPSE_POL['VALUE'].shift(1).fillna(0)
    SYNAPSE_POL = SYNAPSE_POL.iloc[1:]
    SYNAPSE_POL['Value'] = SYNAPSE_POL['VALUE'] - SYNAPSE_POL['VALUE_QK']
    SYNAPSE_POL['Name'] = 'Polygon'
    #avalanche
    SYNAPSE_Ava = SYNAPSE[SYNAPSE['EXPLORER']=='Avalanche']
    SYNAPSE_Ava = SYNAPSE_Ava.reset_index()

    SYNAPSE_Ava = pd.DataFrame(SYNAPSE_Ava.groupby(['TIMESTAMP'])['VALUE'].sum())
    SYNAPSE_Ava['VALUE_QK'] = SYNAPSE_Ava['VALUE'].shift(1).fillna(0)
    SYNAPSE_Ava = SYNAPSE_Ava.iloc[1:]
    SYNAPSE_Ava['Value'] = SYNAPSE_Ava['VALUE'] - SYNAPSE_Ava['VALUE_QK']
    SYNAPSE_Ava['Name'] = 'Avalanche'

    #fantom
    SYNAPSE_fantom = SYNAPSE[SYNAPSE['EXPLORER']=='Fantom']
    SYNAPSE_fantom = SYNAPSE_fantom.reset_index()

    SYNAPSE_fantom = pd.DataFrame(SYNAPSE_fantom.groupby(['TIMESTAMP'])['VALUE'].sum())
    SYNAPSE_fantom['VALUE_QK'] = SYNAPSE_fantom['VALUE'].shift(1).fillna(0)
    SYNAPSE_fantom = SYNAPSE_fantom.iloc[1:]
    SYNAPSE_fantom['Value'] = SYNAPSE_fantom['VALUE'] - SYNAPSE_fantom['VALUE_QK']
    SYNAPSE_fantom['Name'] = 'Fantom'

    #optimism
    SYNAPSE_OPT = SYNAPSE[SYNAPSE['EXPLORER']=='Optimsm']
    SYNAPSE_OPT = SYNAPSE_OPT.reset_index()

    SYNAPSE_OPT = pd.DataFrame(SYNAPSE_OPT.groupby(['TIMESTAMP'])['VALUE'].sum())
    SYNAPSE_OPT['VALUE_QK'] = SYNAPSE_OPT['VALUE'].shift(1).fillna(0)
    SYNAPSE_OPT = SYNAPSE_OPT.iloc[1:]
    SYNAPSE_OPT['Value'] = SYNAPSE_OPT['VALUE'] - SYNAPSE_OPT['VALUE_QK']
    SYNAPSE_OPT['Name'] = 'Optimsm'   
    #metis
    SYNAPSE_METIS = SYNAPSE[SYNAPSE['EXPLORER']=='Metis']
    SYNAPSE_METIS = SYNAPSE_METIS.reset_index()

    SYNAPSE_METIS = pd.DataFrame(SYNAPSE_METIS.groupby(['TIMESTAMP'])['VALUE'].sum())
    SYNAPSE_METIS['VALUE_QK'] = SYNAPSE_METIS['VALUE'].shift(1).fillna(0)
    SYNAPSE_METIS = SYNAPSE_METIS.iloc[1:]
    SYNAPSE_METIS['Value'] = SYNAPSE_METIS['VALUE'] - SYNAPSE_METIS['VALUE_QK']
    SYNAPSE_METIS['Name'] = 'Metis'   
    concat_df_synapse = pd.concat([SYNAPSE_ETH,SYNAPSE_POL,SYNAPSE_Ava,SYNAPSE_fantom,SYNAPSE_OPT,SYNAPSE_METIS],axis=0).reset_index()
    cols =['TIMESTAMP','Value','Name']
    concat_df_synapse = concat_df_synapse[cols]
    return concat_df_synapse

