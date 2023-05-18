from fastapi import APIRouter
from Overviews_Data.Cex import df
from Overviews_Data.Dex import DAI_line, LUSD_line, TUSD_line

overview_router = APIRouter(
    prefix='/overview',
    tags=['overview']
)


@overview_router.get('/Cex')
async def Cex_stablecoin():
    return df.to_dict(orient='records')


@overview_router.get('/Bridge')
async def Bridge():
    return {'message': 'hello '}


@overview_router.get('/Dai')
async def Dai():
    return DAI_line.to_dict(orient='records')


@overview_router.get('/Lusd')
async def Lusd():
    return LUSD_line.to_dict(orient='records')


@overview_router.get('/tusd')
async def tusd():
    return TUSD_line.to_dict(orient='records')
