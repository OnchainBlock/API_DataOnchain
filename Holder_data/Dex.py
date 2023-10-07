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
query_dai_main = os.environ['query_dai_main']
DAI = pd.read_sql(query_dai_main, my_server)
DAI['TIMESTAMP'] = DAI['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
DAI["TIMESTAMP"] = pd.to_datetime(DAI['TIMESTAMP'])
DAI = DAI.set_index('TIMESTAMP')

query_lusd = os.environ['query_lusd_main']
LUSD = pd.read_sql(query_lusd, my_server)
LUSD['TIMESTAMP'] = LUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
LUSD['TIMESTAMP'] = pd.to_datetime(LUSD['TIMESTAMP'])
LUSD_pie = LUSD[LUSD['TIMESTAMP'] ==
                LUSD['TIMESTAMP'].max()][['BALANCE', 'VALUE']].rename(columns={'BALANCE':'label','VALUE':'value'})
LUSD = LUSD.set_index('TIMESTAMP')
# TrueUSD
query_tusd = os.environ['query_tusd_main']
TUSD = pd.read_sql(query_tusd, my_server)

TUSD['TIMESTAMP'] = TUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
TUSD["TIMESTAMP"] = pd.to_datetime(TUSD['TIMESTAMP'])
TUSD = TUSD.set_index('TIMESTAMP')

DAI['href'] = DAI['href'].fillna("hyperlink")
LUSD['href'] = LUSD['href'].fillna("hyperlink")
TUSD['href'] = TUSD['href'].fillna("hyperlink")

def Top_10_Tusd(df):
    LUSD_QK = df.between_time('6:00', '10:59')
    LUSD_QK = LUSD_QK[LUSD_QK['href'] != "hyperlink"].reset_index()
    LUSD_QK['TIMESTAMP'] = pd.to_datetime(LUSD_QK['TIMESTAMP']).dt.date

    L_df = LUSD_QK[LUSD_QK['TIMESTAMP'] ==
                    LUSD_QK['TIMESTAMP'].max() - datetime.timedelta(days=1)]
    cols_last = ['BALANCE', 'VALUE']
    L_df = L_df[cols_last].rename(
        columns={'BALANCE': 'BALANCE_QK', 'VALUE': 'VL_QK'}).reset_index()
    df_present = df.reset_index()
    df_present = df_present[(df_present['TIMESTAMP'] == df_present['TIMESTAMP'].max()) & (
        df_present['href'] != "hyperlink")]

    df_present = df_present[['BALANCE', 'VALUE','href']].reset_index()



    DAY_CHANGE = pd.concat([df_present, L_df], axis=1)


    similar_balance = DAY_CHANGE[DAY_CHANGE['BALANCE']
                                    == DAY_CHANGE['BALANCE_QK']]
    difference_balance = DAY_CHANGE[DAY_CHANGE['BALANCE']
                                    != DAY_CHANGE['BALANCE_QK']]
    difference_balance['PERCENTAGE_FORMAT'] = 'new'
    cols_diff = ['BALANCE', 'VALUE', 'PERCENTAGE_FORMAT']
    difference_balance = difference_balance[cols_diff]
    similar_balance['PERCENTAGE'] = round(
        ((similar_balance['VALUE'] - similar_balance['VL_QK'])/similar_balance['VL_QK'])*100, 2)

    similar_balance = similar_balance[['BALANCE', 'VALUE', 'PERCENTAGE','href']]
    cols_similar = ['BALANCE', 'VALUE', 'PERCENTAGE','href']
    similar_balance = similar_balance[cols_similar]
    # top_10addr =similar_balance.append(difference_balance)
    top_10addr = pd.concat([similar_balance, difference_balance])
    top_10addr = top_10addr.sort_values(by=['VALUE'], ascending=False)
    cols_top10 = ['BALANCE', 'VALUE', 'PERCENTAGE','href']
    top_10addr = top_10addr[cols_top10].rename(columns={'BALANCE':'NAME'})
    top_10addr['PERCENTAGE'] = top_10addr['PERCENTAGE'].fillna("new")
    top_10addr['href'] = top_10addr['href'].fillna('hyperlink')
    return top_10addr

def Top_10_holders(df):
    '''APIRouter
        Include : DAI & LUSD
    '''
    LUSD_QK = df.between_time('6:00', '10:59')
    LUSD_QK = LUSD_QK[LUSD_QK['BALANCE'] != "TOTAL_ASSETS"].reset_index()
    LUSD_QK['TIMESTAMP'] = pd.to_datetime(LUSD_QK['TIMESTAMP']).dt.date
    L_df = LUSD_QK[LUSD_QK['TIMESTAMP'] ==
                    LUSD_QK['TIMESTAMP'].max() - datetime.timedelta(days=1)]
    cols_last = ['BALANCE', 'VALUE']
    L_df = L_df[cols_last].rename(
        columns={'BALANCE': 'BALANCE_QK', 'VALUE': 'VL_QK'}).reset_index()
    df_present = df.reset_index()
    df_present = df_present[(df_present['TIMESTAMP'] == df_present['TIMESTAMP'].max()) & (
        df_present['BALANCE'] != "TOTAL_ASSETS")]

    df_present = df_present[['BALANCE', 'VALUE','href']].reset_index()


    DAY_CHANGE = pd.concat([df_present, L_df], axis=1)

    similar_balance = DAY_CHANGE[DAY_CHANGE['BALANCE']
                                    == DAY_CHANGE['BALANCE_QK']]
    difference_balance = DAY_CHANGE[DAY_CHANGE['BALANCE']
                                    != DAY_CHANGE['BALANCE_QK']]
    difference_balance['PERCENTAGE_FORMAT'] = 'new'
    cols_diff = ['BALANCE', 'VALUE', 'PERCENTAGE_FORMAT']
    difference_balance = difference_balance[cols_diff]
    similar_balance['PERCENTAGE'] = round(
        ((similar_balance['VALUE'] - similar_balance['VL_QK'])/similar_balance['VL_QK'])*100, 2)

    similar_balance = similar_balance[['BALANCE', 'VALUE', 'PERCENTAGE','href']]
    cols_similar = ['BALANCE', 'VALUE', 'PERCENTAGE','href']
    similar_balance = similar_balance[cols_similar]
    # top_10addr =similar_balance.append(difference_balance)
    top_10addr = pd.concat([similar_balance, difference_balance])
    top_10addr = top_10addr.sort_values(by=['VALUE'], ascending=False)
    cols_top10 = ['BALANCE', 'VALUE', 'PERCENTAGE','href']
    top_10addr = top_10addr[cols_top10].rename(columns={'BALANCE':'NAME'})
    top_10addr['PERCENTAGE'] = top_10addr['PERCENTAGE'].fillna("new")
    top_10addr['href'] = top_10addr['href'].fillna('hyperlink')
    return top_10addr


