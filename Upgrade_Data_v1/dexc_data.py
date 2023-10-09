import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *

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
TUSD.loc[TUSD['href']=='None','href'] = 'hyperlink'

def create_table(data):
    # just althorim for DAI, LUSD
    data = data.reset_index()
    hientai = data[(data['TIMESTAMP']==data['TIMESTAMP'].max()) &(data['BALANCE']=='TOTAL_ASSETS')]
    data = data.set_index('TIMESTAMP')
    QK_Data = data.between_time('6:00', '10:59')
    QK_Data = QK_Data.reset_index()
    QK_Data['TIMESTAMP'] = pd.to_datetime(QK_Data['TIMESTAMP']).dt.date
    QK_Data = QK_Data[QK_Data['BALANCE']=='TOTAL_ASSETS']
    lastday = QK_Data[QK_Data['TIMESTAMP']==QK_Data['TIMESTAMP'].max() - datetime.timedelta(days=1)]
    last_week = QK_Data[QK_Data['TIMESTAMP']==QK_Data['TIMESTAMP'].max() - datetime.timedelta(days=7)]
    last_month = QK_Data[QK_Data['TIMESTAMP']==QK_Data['TIMESTAMP'].max() - datetime.timedelta(days=30)]
    df_table = pd.DataFrame({
        '24h_changeVL':[float(hientai['VALUE'])-float(lastday['VALUE'])],
        '24h_per':[((float(hientai['VALUE'])-float(lastday['VALUE']))/float(hientai['VALUE']))*100],
        '7D_changeVL':[float(hientai['VALUE'])-float(last_week['VALUE'])],
        '7D_per':[((float(hientai['VALUE'])-float(last_week['VALUE']))/float(hientai['VALUE']))*100],
        '30D_changeVL':[float(hientai['VALUE'])-float(last_month['VALUE'])],
        '30D_per':[((float(hientai['VALUE'])-float(last_month['VALUE']))/float(hientai['VALUE']))*100],
    })
    return df_table.to_dict(orient='records')

def create_table_tusd(data):
    data = data.reset_index()
    hientai = data[(data['TIMESTAMP']==data['TIMESTAMP'].max())&(data['href']=='hyperlink')]['VALUE'].sum()

    data = data.set_index('TIMESTAMP')
    QK_Data = data.between_time('6:00', '10:59')
    QK_Data = QK_Data.reset_index()
    QK_Data['TIMESTAMP'] = pd.to_datetime(QK_Data['TIMESTAMP']).dt.date
    last_day = QK_Data[(QK_Data['TIMESTAMP'] == QK_Data['TIMESTAMP'].max()-datetime.timedelta(days=1))&(QK_Data['href']=='hyperlink')]['VALUE'].sum()
    last_week = QK_Data[(QK_Data['TIMESTAMP'] == QK_Data['TIMESTAMP'].max()-datetime.timedelta(days=7))&(QK_Data['href']=='hyperlink')]['VALUE'].sum()
    last_month = QK_Data[(QK_Data['TIMESTAMP'] == QK_Data['TIMESTAMP'].max()-datetime.timedelta(days=30))&(QK_Data['href']=='hyperlink')]['VALUE'].sum()
    df_table = pd.DataFrame({
        '24h_changeVL':[float(hientai)-float(last_day)],
        '24h_per':[((float(hientai)-float(last_day))/float(hientai))*100],
        '7D_changeVL':[float(hientai)-float(last_week)],
        '7D_per':[((float(hientai)-float(last_week))/float(hientai))*100],
        '30D_changeVL':[float(hientai)-float(last_month)],
        '30D_per':[((float(hientai)-float(last_month))/float(hientai))*100],
    })
    return df_table.to_dict(orient='records')
 
  
def statistic_dexc(token:str):
    choice_condition = ['Dai','Lusd','Tusd']
    if token not in choice_condition:
        return f'balance: {token} is not found, plase choice another ["Dai","Lusd","Tusd"]'
    elif token=="Dai":
        return create_table(DAI)
    elif token=='Lusd':
        return create_table(LUSD)
    elif token=='Tusd':
        return create_table_tusd(TUSD)


