from imports import *
from Holder_data.Dex import *
from Holder_data.Dex import DAI,LUSD,TUSD
Holder_router = APIRouter(
    prefix='/holder',
    tags=['holder']
)


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
        return holder_lusd.to_dict(orient='records')
    elif Dex_name=="Tusd":
        holder_tusd = Top_10_holders(TUSD)
        return holder_tusd.to_dict(orient='records')


