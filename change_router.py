from fastapi import APIRouter
change_router = APIRouter(
    prefix='/change',
    tags=['Net flow : Deposit-withdraws']
)


@change_router.get('/Bridge/Multichain')
async def Multichain():
    return {'message': 'Change Multichian '}


@change_router.get('/Bridge/Hop')
async def Hop():
    return {'message': 'Change Hop '}


@change_router.get('/bridge/Stargate')
async def Stargate():
    return {'message': 'Change Stargate '}


@change_router.get('/bridge/Celer')
async def Celer():
    return {'message': 'Change Celer '}


@change_router.get('/bridge/Synapse')
async def Synapse():
    return {'message': 'Change Synapse '}
