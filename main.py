# from fastapi import FastAPI
# import uvicorn
from imports import *
# from change_router import change_router
# from Distribution_router import distribution_router
# from Holder_dex import Holder_router
# from Overview_router import overview_router
# from ETH_router import eth_router
# # from fastapi.openapi.utils import get_openapi
# from Reserve_router import Reserve_router
# from Eth_bridge_router import eth_bridge_router
from Stablecoin_router import stablecoin_v1_router

app = FastAPI()


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="API Data Onchain",
        version="v1.0",
        description="An API for Data Onchain service",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema
origins = [
    "https://api.cryptoviet.info",
    "https://cryptoviet.site",
    "https://cryptoviet.info",
    "https://data.cryptoviet.info",
    "http://45.76.183.129:3000",
    "http://localhost:3000",
    "https://cryptoviet-info.vercel.app",
    "https://dataonchain.xyz",
    "https://onchainblock.xyz",
    "45.76.183.129:3333"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.openapi = custom_openapi


# app.include_router(overview_router)
# app.include_router(distribution_router)
# app.include_router(Reserve_router)
# app.include_router(change_router)
# app.include_router(Holder_router)
# app.include_router(eth_router)
# app.include_router(eth_bridge_router)
app.include_router(stablecoin_v1_router)
if __name__ == '__main__':
    uvicorn.run(app,port='8000')
