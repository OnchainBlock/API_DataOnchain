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
        Deposit_multichain = Deposit_multichain[Deposit_multichain['TIMESTAMP'].between(start,end)]
        return Deposit_multichain.to_dict(orient="records")
    elif bridge_name =="Celer":
        Deposit_celer = create_df_deposit_celer(Celer_cBridge)
        Deposit_celer = rename(Deposit_celer)
        Deposit_celer = Deposit_celer[Deposit_celer['TIMESTAMP'].between(start,end)]
        return Deposit_celer.to_dict(orient='records')
    elif bridge_name=="Hop":
        Deposit_hopbridge = create_df_deposit_hop(Deposit_hop)
        Deposit_hopbridge = rename(Deposit_hopbridge)
        Deposit_hopbridge = Deposit_hopbridge[Deposit_hopbridge['TIMESTAMP'].between(start,end)]
        return Deposit_hopbridge.to_dict(orient="records")
    elif bridge_name =="Stargate":
        Deposit_stargate = create_df_deposit_stargate(STARGATE)
        Deposit_stargate = rename(Deposit_stargate)
        Deposit_stargate = Deposit_stargate[Deposit_stargate['TIMESTAMP'].between(start,end)]
        return Deposit_stargate.to_dict(orient='records')
    elif bridge_name=="Synapse":
        Deposit_synapse = create_df_deposit_synapse(SYNAPSE)
        Deposit_synapse = Deposit_synapse[Deposit_synapse['TIMESTAMP'].between(start,end)]
        return Deposit_synapse.to_dict(orient='records')

