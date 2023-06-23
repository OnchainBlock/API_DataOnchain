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
query_dai = os.environ['query_dai']
DAI = pd.read_sql(query_dai, my_server)
DAI['TIMESTAMP'] = DAI['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
DAI["TIMESTAMP"] = pd.to_datetime(DAI['TIMESTAMP'])
DAI = DAI.set_index('TIMESTAMP')


def Dai_line(DAI):

    Dai_line_df = DAI[DAI['BALANCE'] == 'TOTAL_ASSETS']
    Dai_line_df = Dai_line_df.sort_index(ascending=True).reset_index()[
        ['TIMESTAMP', 'VALUE']]
    return Dai_line_df


DAI_pie = DAI[DAI.index == DAI.index.max()].reset_index()[['BALANCE', 'VALUE']]
another = DAI_pie[DAI_pie['BALANCE'] == 'TOTAL_ASSETS']['VALUE'] - \
    DAI_pie[DAI_pie['BALANCE'] != 'TOTAL_ASSETS']['VALUE'].sum()
another = pd.DataFrame({
    'BALANCE': 'another',
    'VALUE': [float(another)],
})

DAI_pie_df = DAI_pie[DAI_pie['BALANCE'] != 'TOTAL_ASSETS']
# DAI_pie_df = DAI_pie_df.append(another, ignore_index=True)
DAI_pie_df = pd.concat([DAI_pie_df, another]).rename(columns={'BALANCE':'label','VALUE':'value'})

# LUSD

query_lusd = os.environ['query_lusd']
LUSD = pd.read_sql(query_lusd, my_server)
LUSD['TIMESTAMP'] = LUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
LUSD['TIMESTAMP'] = pd.to_datetime(LUSD['TIMESTAMP'])
LUSD_pie = LUSD[LUSD['TIMESTAMP'] ==
                LUSD['TIMESTAMP'].max()][['BALANCE', 'VALUE']].rename(columns={'BALANCE':'label','VALUE':'value'})



def lusd_line(LUSD):
    LUSD_line = LUSD[LUSD['LABEL'] == "TOTAL ASSETS"][['TIMESTAMP', 'VALUE']]
    return LUSD_line


# TrueUSD
query_tusd = os.environ['query_tusd']
TUSD = pd.read_sql(query_tusd, my_server)

TUSD['TIMESTAMP'] = TUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
TUSD["TIMESTAMP"] = pd.to_datetime(TUSD['TIMESTAMP'])
TUSD = TUSD.set_index('TIMESTAMP')


def Tusd_line(TUSD):

    TUSD_line = TUSD[TUSD['LABEL'] == "EXPLORER"]
    TUSD_line = TUSD_line.reset_index()
    TUSD_line = TUSD_line.groupby(['TIMESTAMP'])['VALUE'].sum()
    TUSD_line = pd.DataFrame(TUSD_line)
    TUSD_line = TUSD_line.reset_index()
    return TUSD_line


df_pie = TUSD[TUSD.index == TUSD.index.max()].reset_index()[
    ['BALANCE', 'VALUE', 'LABEL']]
another = df_pie[df_pie['LABEL'] == 'EXPLORER']['VALUE'].sum(
) - df_pie[df_pie['LABEL'] != "EXPLORER"]['VALUE'].sum()
another = pd.DataFrame({
    'BALANCE': ['another'],
    'VALUE': [another],

})
Tusd_pie = df_pie[df_pie['LABEL'] != "EXPLORER"]
# Tusd_pie = Tusd_pie.append(another,ignore_index=True)
Tusd_pie = pd.concat([Tusd_pie, another])[['BALANCE', 'VALUE']].rename(columns={'BALANCE':'label','VALUE':'value'})

Dex_pie = pd.DataFrame({
    # 'Dai':DAI_pie[DAI_pie['BALANCE']=="TOTAL_ASSETS"]['VALUE'].values,
    # 'Lusd':[sum(LUSD_pie['value'])],
    # 'Tusd':[sum(df_pie['VALUE'])]
    'label':['Dai','Lusd','Tusd'],
    'value':[DAI_pie[DAI_pie['BALANCE']=="TOTAL_ASSETS"]['VALUE'].values[0],sum(LUSD_pie['value']),sum(df_pie['VALUE'])]
})

