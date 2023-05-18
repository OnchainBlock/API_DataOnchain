from imports import *
Holder_router = APIRouter(
    prefix='/holder',
    tags=['holder']
)


@Holder_router.get('/Dai')
async def holder():
    return {'message': 'hello '}


@Holder_router.get('/Tusd')
async def holder():
    return {'message': 'hello '}


@Holder_router.get('/Lusd')
async def holder():
    return {'message': 'hello '}
