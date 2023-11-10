import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *

server = os.environ['my_server']
fdusd_query = os.environ['query_FDUSD']

fdusd =pd.read_sql(fdusd_query,server)
# config format time
fdusd['timestamp'] = fdusd['timestamp'].apply(
    lambda x: pd.to_datetime(x).floor('T'))

class Funtions_FDUSD():
    def __init__(self) -> None:
        pass
    def create_pie():
        data =fdusd[fdusd['timestamp']==fdusd['timestamp'].max()]
        data = data.groupby(['chain']).agg({'value':'sum'}).reset_index()
        return data.to_dict(orient='records')
    # create chart 
    def cre_df_netflow(start:str,end:str,data):
        df_netFlow =data.groupby(['timestamp']).agg({'value':'sum'}).reset_index()
        df_netFlow['qk_value'] = df_netFlow['value'].shift(1).fillna(0)
        df_netFlow = df_netFlow.iloc[2:]

        df_netFlow['change'] = round(df_netFlow['value'] - df_netFlow['qk_value'],2)
        df_netFlow['time_select'] = pd.to_datetime(df_netFlow['timestamp']).dt.date
        df_netFlow['time_select'] = pd.to_datetime(df_netFlow['time_select'])
        df_netFlow = df_netFlow[df_netFlow['time_select'].between(start,end)]
        cols =['timestamp','value','change']
        df_netFlow = df_netFlow[cols]
        return df_netFlow.to_dict(orient='records')

