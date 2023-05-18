import os
from sqlalchemy import create_engine
from dotenv.main import load_dotenv
import pandas as pd
load_dotenv()
my_server = os.environ['my_server']
query_cex = os.environ['query_cex']

my_server = create_engine(my_server)
df = pd.read_sql(query_cex, my_server)
df['TimeStamp'] = df['TimeStamp'].apply(lambda x: pd.to_datetime(x).floor('T'))
df = df.fillna(0)


df['SUM'] = df['USDT'] + df['USDC']+df['BUSD']

df = df[['TimeStamp', 'Symbols', 'SUM']]
df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])
