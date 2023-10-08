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

# TrueUSD
query_tusd = os.environ['query_tusd_main']
TUSD = pd.read_sql(query_tusd, my_server)

TUSD['TIMESTAMP'] = TUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
TUSD["TIMESTAMP"] = pd.to_datetime(TUSD['TIMESTAMP'])
TUSD = TUSD.set_index('TIMESTAMP')


def Dai_line(DAI):
    
    Dai_line_df = DAI[DAI['BALANCE'] == 'TOTAL_ASSETS']
    Dai_line_df = Dai_line_df.sort_index(ascending=True).reset_index()[
        ['TIMESTAMP', 'VALUE']]
    return Dai_line_df

def lusd_line(LUSD):
    LUSD_line = LUSD[LUSD['BALANCE'] == "TOTAL_ASSETS"][['TIMESTAMP', 'VALUE']]
    return LUSD_line

def Tusd_line(TUSD):

    TUSD_line = TUSD[TUSD['href'] == "hyperlink"]
    TUSD_line = TUSD_line.reset_index()
    TUSD_line = TUSD_line.groupby(['TIMESTAMP'])['VALUE'].sum()
    TUSD_line = pd.DataFrame(TUSD_line)
    TUSD_line = TUSD_line.reset_index()
    return TUSD_line

DAI['href'] = DAI['href'].fillna("hyperlink")
LUSD['href'] = LUSD['href'].fillna("hyperlink")
TUSD['href'] = TUSD['href'].fillna("hyperlink")
TUSD.loc[TUSD['href']=='None','href'] = 'hyperlink'

#Daipie
DAI_pie = DAI[DAI.index == DAI.index.max()].reset_index()[['BALANCE', 'VALUE']]
another = DAI_pie[DAI_pie['BALANCE'] == 'TOTAL_ASSETS']['VALUE'] - \
    DAI_pie[DAI_pie['BALANCE'] != 'TOTAL_ASSETS']['VALUE'].sum()
another = pd.DataFrame({
    'BALANCE': 'another',
    'VALUE': [float(another)],
})
DAI_pie_df = DAI_pie[DAI_pie['BALANCE'] != 'TOTAL_ASSETS']
DAI_pie_df = pd.concat([DAI_pie_df, another]).rename(columns={'BALANCE':'label','VALUE':'value'})

#LUSD
LUSD_pie = LUSD[LUSD['TIMESTAMP'] ==
                LUSD['TIMESTAMP'].max()][['BALANCE', 'VALUE']]
another = LUSD_pie[LUSD_pie['BALANCE'] == 'TOTAL_ASSETS']['VALUE'] - \
    LUSD_pie[LUSD_pie['BALANCE'] != 'TOTAL_ASSETS']['VALUE'].sum()
another = pd.DataFrame({
    'BALANCE': 'another',
    'VALUE': [float(another)],
})
LUSD_pie_df = LUSD_pie[LUSD_pie['BALANCE'] != 'TOTAL_ASSETS']
LUSD_pie_df = pd.concat([LUSD_pie_df, another]).rename(columns={'BALANCE':'label','VALUE':'value'})

#TUSD
df_pie  = TUSD[TUSD.index == TUSD.index.max()].reset_index()[['BALANCE', 'VALUE','href']]
df_pie.loc[df_pie['href']=="None",'href'] = 'hyperlink'
another = df_pie[df_pie['href'] == 'hyperlink']['VALUE'].sum(
) - df_pie[df_pie['href'] != "hyperlink"]['VALUE'].sum()
another = pd.DataFrame({
    'BALANCE': ['another'],
    'VALUE': [another],

})

#end
Tusd_pie = df_pie[df_pie['href'] != "hyperlink"]

Tusd_pie = pd.concat([Tusd_pie, another])[['BALANCE', 'VALUE']].rename(columns={'BALANCE':'label','VALUE':'value'})


    
# pie all 
Dex_pie = pd.DataFrame({
    # 'Dai':DAI_pie[DAI_pie['BALANCE']=="TOTAL_ASSETS"]['VALUE'].values,
    # 'Lusd':[sum(LUSD_pie['value'])],
    # 'Tusd':[sum(df_pie['VALUE'])]
    'label':['Dai','Lusd','Tusd'],
    'value':[DAI_pie_df['value'].sum(),sum(LUSD_pie_df['value']),sum(Tusd_pie['value'])]
})



