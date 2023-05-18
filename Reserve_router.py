from fastapi import APIRouter
Reserve_router = APIRouter(
    prefix='/reserve',
    tags=['reserve']
)


@Reserve_router.get('/Binance')
async def reserve():
    return {'message': 'hello '}


@Reserve_router.get('/tier1')
async def reserve():
    return {'message': 'hello '}


@Reserve_router.get('/tier2')
async def reserve():
    return {'message': 'hello '}


@Reserve_router.get('/tier3')
async def reserve():
    return {'message': 'hello '}
