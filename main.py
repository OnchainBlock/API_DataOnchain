import sys
sys.path.append(r'/Users/dev/Thang_DataEngineer/API_DataOnchain')
from imports import *
from Router.change_router import change_router
from Router.Distribution_router import distribution_router
from Router.Holder_dex import Holder_router
from Router.Overview_router import overview_router
from Router.ETH_router import eth_router
from Router.Reserve_router import Reserve_router
from Router.Eth_bridge_router import eth_bridge_router
from Router.Stablecoin_router import stablecoin_v1_router
from Router.L2_tx_overview_router import l2_tx_router
from Router.l2_arbitrum_router import arbitrum_router
from Router.l2_starknet_router import starknet_router
from Router.l2_zksync_era_router import zksync_router
from Router.l2_optimism_router import optimism_router
from Router.l2_polygon_router import polygon_router
from Router.l2_base_router import base_router
from Router.l2_linea_router import linea_router
from Router.l2_mantle_router import mantle_router
from Router.l2_manta_router import manta_router

app = FastAPI()


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="OnchainBlock Provide API",
        version="v1.1.1",
        description="Provide API for dev web3",
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
    # CORSMiddleware,
    # allow_origins=origins,
    # allow_credentials=True,
    # allow_methods=["*"],
    # allow_headers=["*"],
    CProfileMiddleware,
    enable=True,
    print_each_request=True,
    strip_dirs=False,
    sort_by="cumtime"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.openapi = custom_openapi


app.include_router(overview_router)
app.include_router(distribution_router)
app.include_router(Reserve_router)
app.include_router(change_router)
app.include_router(Holder_router)
app.include_router(eth_router)
app.include_router(eth_bridge_router)
app.include_router(stablecoin_v1_router)
app.include_router(l2_tx_router)
app.include_router(arbitrum_router)
app.include_router(starknet_router)
app.include_router(zksync_router)
app.include_router(optimism_router)
app.include_router(polygon_router)
app.include_router(base_router)
app.include_router(linea_router)
app.include_router(mantle_router)
app.include_router(manta_router)
if __name__ == '__main__':
    uvicorn.run(app,host='localhost')

