from imports import *
from Holder_data.Dex import *
Holder_router = APIRouter(
    prefix='/holder',
    tags=['holder']
)


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

cols = ['BALANCE','VALUE','href']
DAI = DAI[cols]
LUSD = LUSD[cols]
TUSD = TUSD[cols]
@Holder_router.get('/Dex')
async def holder(Dex_name:str):
    choice_condition = ['Dai','Lusd','Tusd']
    if Dex_name not in choice_condition:
        return f'Dex_name: {Dex_name} is not found, plase choice another ["Dai","Lusd","Tusd"]'
    elif Dex_name =="Dai":
        holder_dai = Top_10_holders(DAI)
        return holder_dai.to_dict(orient='records')
    elif Dex_name=="Lusd":
        holder_lusd = Top_10_holders(LUSD)
        holder_lusd = holder_lusd.to_dict(orient='records')
        return holder_lusd
    elif Dex_name=="Tusd":
        holder_tusd = Top_10_Tusd(TUSD)
        holder_tusd = holder_tusd.iloc[:10]
        return holder_tusd.to_dict(orient='records')

