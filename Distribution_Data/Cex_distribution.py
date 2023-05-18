from imports import *


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


def create_df_Treemap(data_qk, data_hientai, chioce_days):
    Hientai_Data = data_hientai[data_hientai['TimeStamp'] == data_hientai['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']

    data_qk['TimeStamp'] = pd.to_datetime(data_qk['TimeStamp']).dt.date

    Last_data = data_qk[(data_qk['TimeStamp'] == data_qk['TimeStamp'].max() - datetime.timedelta(days=chioce_days))
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

    TREE_CHANGE_USDT = Funtion_Col_Processing(DATA_CHANGE,  'Symbols', 'USDT', f'{chioce_days}D_USDT', 'USDT').rename(
        columns={'USDT': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE', 'VALUE_SHOW', 'STABLECOIN']]
    TREE_CHANGE_USDC = Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'USDC', f'{chioce_days}D_USDC', 'USDC').rename(
        columns={'USDC': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE', 'VALUE_SHOW', 'STABLECOIN']]
    TREE_CHANGE_BUSD = Funtion_Col_Processing(DATA_CHANGE, 'Symbols', 'BUSD', f'{chioce_days}D_BUSD', 'BUSD').rename(
        columns={'BUSD': 'VALUE'})[['Symbols', 'VALUE', 'PERCENTAGE', 'VALUE_SHOW', 'STABLECOIN']]
    TREE_CHANGE = pd.concat(
        [TREE_CHANGE_USDT, TREE_CHANGE_USDC, TREE_CHANGE_BUSD])
    return TREE_CHANGE


def create_df_Treemap_sum(data_qk, data_hientai, choice_days):
    Hientai_Data = data_hientai[data_hientai['TimeStamp'] == data_hientai['TimeStamp'].max()][[
        'Symbols', 'USDT', 'USDC', 'BUSD']]
    Hientai_Data = Hientai_Data.reset_index()
    Hientai_Data['ALL_HIENTAI'] = Hientai_Data['USDT'] + \
        Hientai_Data['USDC'] + Hientai_Data['BUSD']

    data_qk['TimeStamp'] = pd.to_datetime(data_qk['TimeStamp']).dt.date

    Last_data = data_qk[(data_qk['TimeStamp'] == data_qk['TimeStamp'].max() - datetime.timedelta(days=choice_days))
                        ][['USDT', 'USDC', 'BUSD']].rename(columns={"USDT": 'USDT_Las', 'USDC': 'USDC_Las', 'BUSD': 'BUSD_Las'})
    Last_data = Last_data.reset_index()
    Last_data['ALL_LAS'] = Last_data['USDT_Las'] + \
        Last_data['USDC_Las'] + Last_data['BUSD_Las']

    DATA_CHANGE = pd.concat([Hientai_Data, Last_data], axis=1)
    DATA_CHANGE = DATA_CHANGE.fillna(0)
    DATA_CHANGE[f'{choice_days}D_USDT'] = (
        (DATA_CHANGE['USDT'] - DATA_CHANGE['USDT_Las'])/DATA_CHANGE['USDT_Las'])*100
    DATA_CHANGE[f'{choice_days}D_USDC'] = (
        (DATA_CHANGE['USDC'] - DATA_CHANGE['USDC_Las'])/DATA_CHANGE['USDC_Las'])*100
    DATA_CHANGE[f'{choice_days}D_BUSD'] = (
        (DATA_CHANGE['BUSD'] - DATA_CHANGE['BUSD_Las'])/DATA_CHANGE['BUSD_Las'])*100
    DATA_CHANGE[f'{choice_days}D_ALL'] = (
        (DATA_CHANGE['ALL_HIENTAI'] - DATA_CHANGE['ALL_LAS'])/DATA_CHANGE['ALL_LAS'])*100
    DATA_CHANGE = DATA_CHANGE.fillna(0)

    DATA_CHANGE_SUM = DATA_CHANGE[['Symbols',
                                   'ALL_HIENTAI', f'{choice_days}D_ALL']]
    DATA_CHANGE_SUM['VALUE_SHOW'] = DATA_CHANGE_SUM['ALL_HIENTAI'].map(
        lambda x: numerize.numerize(x, 2))
    DATA_CHANGE_SUM.drop(
        DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(
        columns={f'{choice_days}D_ALL': 'PERCENTAGE'})
    DATA_CHANGE_SUM['PERCENTAGE'] = DATA_CHANGE_SUM['PERCENTAGE'].map(
        lambda x: round(x, 2))
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.drop(
        DATA_CHANGE_SUM[DATA_CHANGE_SUM['ALL_HIENTAI'] == 0.].index)
    DATA_CHANGE_SUM = DATA_CHANGE_SUM.rename(columns={'ALL_HIENTAI': 'VALUE'})
    return DATA_CHANGE_SUM
