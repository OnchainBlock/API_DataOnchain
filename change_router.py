from fastapi import APIRouter
from Deposit_Data.Bridge import *
from Deposit_Data.Bridge import Deposit_mul,Deposit_hop,Celer_cBridge,STARGATE,SYNAPSE
change_router = APIRouter(
    prefix='/change',
    tags=['Net flow : Deposit-withdraws']
)


@change_router.get('/Bridge')
async def choice_Bridge(start:str,end:str,bridge_name:str):
    choice_condition = ['Multichain','Celer','Hop','Stargate','Synapse']
    if bridge_name not in choice_condition:
        return f'bridge_name: {bridge_name} is not found, plase choice another ["Multichain","Celer","Hop","Stargate","Synapse"]'
    elif bridge_name =="Multichain":
        Deposit_multichain = create_Deposit_multichain(Deposit_mul)
        Deposit_multichain = rename(Deposit_multichain)
        Deposit_multichain['time'] = pd.to_datetime(Deposit_multichain['TIMESTAMP']).dt.date
        Deposit_multichain['time'] = pd.to_datetime(Deposit_multichain['time'])
        Deposit_multichain = Deposit_multichain[Deposit_multichain['time'].between(start,end)].drop(columns={'time'})
        Deposit_multichain = Deposit_multichain.rename(columns={'TIMESTAMP':'timestamp','Value':'value','Name':'label'})
        return Deposit_multichain.to_dict(orient='records')
    elif bridge_name =="Celer":
        Deposit_celer = create_df_deposit_celer(Celer_cBridge)
        Deposit_celer = rename(Deposit_celer)
        Deposit_celer['time'] = pd.to_datetime(Deposit_celer['TIMESTAMP']).dt.date
        Deposit_celer['time'] = pd.to_datetime(Deposit_celer['time'])
        Deposit_celer = Deposit_celer[Deposit_celer['time'].between(start,end)].drop(columns={'time'})
        Deposit_celer = Deposit_celer.rename(columns={'TIMESTAMP':'timestamp','Value':'value','Name':'label'})
        return Deposit_celer.to_dict(orient='records')
    elif bridge_name=="Hop":
        Deposit_hopbridge = create_df_deposit_hop(Deposit_hop)
        Deposit_hopbridge = rename(Deposit_hopbridge)
        Deposit_hopbridge['time'] = pd.to_datetime(Deposit_hopbridge['TIMESTAMP']).dt.date
        Deposit_hopbridge['time'] = pd.to_datetime(Deposit_hopbridge['time'])
        Deposit_hopbridge = Deposit_hopbridge[Deposit_hopbridge['time'].between(start,end)].drop(columns={'time'})
        Deposit_hopbridge = Deposit_hopbridge.rename(columns={'TIMESTAMP':'timestamp','Value':'value','Name':'label'})
        return Deposit_hopbridge.to_dict(orient="records")
    elif bridge_name =="Stargate":
        Deposit_stargate = create_df_deposit_stargate(STARGATE)
        Deposit_stargate = rename(Deposit_stargate)
        Deposit_stargate['time'] = pd.to_datetime(Deposit_stargate['TIMESTAMP']).dt.date
        Deposit_stargate['time'] = pd.to_datetime(Deposit_stargate['time'])
        Deposit_stargate = Deposit_stargate[Deposit_stargate['time'].between(start,end)].drop(columns={'time'})
        Deposit_stargate = Deposit_stargate.rename(columns={'TIMESTAMP':'timestamp','Value':'value','Name':'label'})
        return Deposit_stargate.to_dict(orient='records')
    elif bridge_name=="Synapse":
        Deposit_synapse = create_df_deposit_synapse(SYNAPSE)
        Deposit_synapse['time'] = pd.to_datetime(Deposit_synapse['TIMESTAMP']).dt.date
        Deposit_synapse['time'] = pd.to_datetime(Deposit_synapse['time'])
        Deposit_synapse = Deposit_synapse[Deposit_synapse['time'].between(start,end)].drop(columns={'time'})
        Deposit_synapse= Deposit_synapse.rename(columns={'TIMESTAMP':'timestamp','Value':'value','Name':'label'})
        return Deposit_synapse.to_dict(orient='records')

