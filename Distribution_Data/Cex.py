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
pd.options.mode.chained_assignment = None
load_dotenv()
my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
data = pd.read_sql(query_cex, my_server)
data['TimeStamp'] = data['TimeStamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))
QK_Data = data.set_index('TimeStamp')
QK_Data = QK_Data.between_time('6:00', '10:59')
QK_Data = QK_Data.reset_index()


def Funtion_Col_Processing(data, symbol_col_name, value_col_name, percentage_col_name, name_Stablecoin):
    '''
    1) Calculative Percentage processing

    '''
    data = data[[symbol_col_name, value_col_name, percentage_col_name]]
    data['VALUE_SHOW'] = data[value_col_name].map(
        lambda x: numerize.numerize(x, 2))
    data = data.drop(data[data[value_col_name] == 0.].index)
    data['STABLECOIN'] = name_Stablecoin
    data = data.rename(columns={percentage_col_name: 'PERCENTAGE'})
    data['PERCENTAGE'] = data['PERCENTAGE'].map(lambda x: round(x, 2))
    return data


# Funtion Color condition
def Format_Color(data):
    '''
    1) Format negative is red color
    2) Format positive value is green color
    '''
    neg = np.array(data['PERCENTAGE'])
    neg = neg[np.where((neg < 0) & (neg > -99))]
    # positive
    pos = np.array(data['PERCENTAGE'])
    pos = pos[np.where((pos > 0) & (pos < 100))]
    max_ne = neg.min()
    max_po = pos.max()

    # filter condition formatign
    conditions = [
        # red nhat
        (data['PERCENTAGE'] >= max_ne/3) & (data['PERCENTAGE'] < 0),
        (data['PERCENTAGE'] >= max_ne/2) & (data['PERCENTAGE'] < max_ne/3),
        (data['PERCENTAGE'] < max_ne/2),



        # green
        (data['PERCENTAGE'] > 0) & (data['PERCENTAGE'] <= max_po/3),
        (data['PERCENTAGE'] > max_po/3) & (data['PERCENTAGE'] <= max_po/2),
        (data['PERCENTAGE'] > max_po/2)
    ]
    choies_character = ['A', 'B', 'C', 'D', 'E', 'F']
    data['CONDITIONS'] = np.select(conditions, choies_character, default='Z')
    data['all'] = 'all'
    return data
# Forcessing total columns


def Funtion_Col_Processing_sum(data, symbol_col_name, value_col_name, percentage_col_name):
    data = data[[symbol_col_name, value_col_name, percentage_col_name]]
    data['VALUE_SHOW'] = data[value_col_name].map(
        lambda x: numerize.numerize(x, 2))
    data.drop(data[data[value_col_name] == 0.].index)
    data = data.rename(columns={percentage_col_name: 'PERCENTAGE'})
    data['PERCENTAGE'] = data['PERCENTAGE'].map(lambda x: round(x, 2))
    data = data.drop(data[data[value_col_name] == 0.].index)
    data = data.rename(columns={value_col_name: 'VALUE'})
    return data
