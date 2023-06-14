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

def Treemap(chioce_days: int, label: str):

    Hientai_Data = data[data['TimeStamp'] == data['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']
        

    QK_Data['TimeStamp'] = pd.to_datetime(QK_Data['TimeStamp']).dt.date

    Last_data = QK_Data[(QK_Data['TimeStamp'] == QK_Data['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
                        ][['USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
    Last_data = Last_data.reset_index()
    Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
        Last_data['USDC_Las'] + Last_data['BUSD_Las']

    DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE[f'{chioce_days}D_USDT'] = (
        (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_USDC'] = (
        (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_BUSD'] = (
        (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD_Las'])*100
    DATA_CHANGE[f'{chioce_days}D_ALL'] = (
        (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_LAS'])*100
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

        BUSD = BUSD.drop(BUSD[BUSD[f'{chioce_days}D_BUSD'] == 0.000].index)
        BUSD[f'{chioce_days}D_BUSD'] = BUSD[f'{chioce_days}D_BUSD'].map(lambda x: round(x, 2))
        BUSD = BUSD.rename(columns={'BUSD':'VALUE',f'{chioce_days}D_BUSD':'PERCENTAGE'})
        size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
        BUSD = BUSD.sort_values(by=['VALUE'],ascending=False)
        BUSD['size'] = [i for i in size[:len(BUSD)]]
        return BUSD
    elif label=='Usdc':
        cols_usdc = ['Symbols','USDC',f'{chioce_days}D_USDC']
        USDC = DATA_CHANGE[cols_usdc]
        USDC = USDC.drop(USDC[USDC[f'{chioce_days}D_USDC'] == 0.].index)
        USDC[f'{chioce_days}D_USDC'] = USDC[f'{chioce_days}D_USDC'].map(lambda x: round(x, 2))
        USDC = USDC.rename(columns={'USDC':'VALUE',f'{chioce_days}D_USDC':'PERCENTAGE'})
        size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
        USDC['size'] = [i for i in size[:len(USDC)]]
        return USDC.to_dict(orient='records')
    elif label=='Usdt':
        cols_usdt = ['Symbols','USDT',f'{chioce_days}D_USDT']
        USDT = DATA_CHANGE[cols_usdt]
        USDT = USDT.drop(USDT[USDT[f'{chioce_days}D_USDT'] == 0.].index)
        USDT[f'{chioce_days}D_USDT'] = USDT[f'{chioce_days}D_USDT'].map(lambda x: round(x, 2))
        USDT = USDT.rename(columns={'USDT':'VALUE',f'{chioce_days}D_USDT':'PERCENTAGE'})
        size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
        USDT['size'] = [i for i in size[:len(USDT)]]
        return USDT.to_dict(orient='records')
    elif label=="Total":
        DATA_CHANGE_SUM = DATA_CHANGE_SUM.sort_values(by = ['VALUE'],ascending=False)
        size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
        DATA_CHANGE_SUM['size'] = [i for i in size[:len(DATA_CHANGE_SUM)]]
        return DATA_CHANGE_SUM.to_dict(orient='records')
print(Treemap(1,'Busd'))


    # if label =='Usdt':
    #     USDT = Funtion_Col_Processing(DATA_CHANGE,  'Symbols', 'USDT', f'{chioce_days}D_USDT', 'USDT').rename(
    #             columns={'USDT': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']]
    #     USDT = USDT.sort_values(by = ['VALUE'],ascending=False)
    #     size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
    #     USDT['size'] = [i for i in size[:len(USDT)]]
    #     return USDT.to_dict(orient='records')
    # elif label =='Usdc':
    #     USDC = Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'USDC', f'{chioce_days}D_USDC', 'USDC').rename(
    #         columns={'USDC': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']]
    #     USDC = USDC.sort_values(by = ['VALUE'],ascending=False)
    #     size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
    #     USDC['size'] = [i for i in size[:len(USDC)]]
    #     return USDC.to_dict(orient='records')
    # elif label=='Busd':
    #     BUSD = Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'BUSD', f'{chioce_days}D_BUSD', 'BUSD').rename(
    #         columns={'BUSD': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE']]
    #     BUSD = BUSD.sort_values(by = ['VALUE'],ascending=False)
    #     size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
    #     BUSD['size'] = [i for i in size[:len(BUSD)]]
    #     return BUSD.to_dict(orient='records')
    # elif label =='Total':
    #     DATA_CHANGE_SUM = DATA_CHANGE_SUM.sort_values(by = ['VALUE'],ascending=False)
    #     size = [250,100,50,30,20,20,20,20,20,20,20,20,15,15,10,5]
    #     DATA_CHANGE_SUM['size'] = [i for i in size[:len(DATA_CHANGE_SUM)]]
    #     return DATA_CHANGE_SUM.to_dict(orient='records')
    # else:
    #     return f'Not found: {label} please choose [Usdt , Usdc, Busd, Total] '



# test

