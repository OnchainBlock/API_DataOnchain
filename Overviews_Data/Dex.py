import os
from sqlalchemy import create_engine
from dotenv.main import load_dotenv
import pandas as pd
load_dotenv()

my_server = os.environ['my_server']
query_dai = os.environ['query_dai']

DAI = pd.read_sql(query_dai, my_server)
DAI['TIMESTAMP'] = DAI['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
DAI["TIMESTAMP"] = pd.to_datetime(DAI['TIMESTAMP'])
DAI_line = DAI[DAI['BALANCE'] == "TOTAL_ASSETS"]
cols_dai = ['TIMESTAMP', 'VALUE']
DAI_line = DAI_line[cols_dai]
# LUSD
query_lusd = os.environ['query_lusd']

LUSD = pd.read_sql(query_lusd, my_server)
LUSD['TIMESTAMP'] = LUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
LUSD['TIMESTAMP'] = pd.to_datetime(LUSD['TIMESTAMP'])
LUSD_line = LUSD[LUSD['LABEL'] == "TOTAL ASSETS"]
cols_lusd = ['TIMESTAMP', 'VALUE']
LUSD_line = LUSD_line[cols_lusd]

# True USD
query_tusd = os.environ['query_tusd']
TUSD = pd.read_sql(query_tusd, my_server)

TUSD['TIMESTAMP'] = TUSD['TIMESTAMP'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
TUSD["TIMESTAMP"] = pd.to_datetime(TUSD['TIMESTAMP'])
TUSD = TUSD.set_index('TIMESTAMP')

''' Line total'''
TUSD_line = TUSD[TUSD['LABEL'] == "EXPLORER"]
TUSD_line = TUSD_line.reset_index()
TUSD_line = TUSD_line.groupby(['TIMESTAMP', 'LABEL'])['VALUE'].sum()
TUSD_line = pd.DataFrame(TUSD_line)
TUSD_line = TUSD_line.reset_index()
cols_tusd = ['TIMESTAMP', 'VALUE']
TUSD_line = TUSD_line[cols_tusd]
