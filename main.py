# from fastapi import FastAPI
# import uvicorn
from imports import *
from change_router import change_router
from Distribution_router import distribution_router
from Holders_router import Holder_router
from Overview_router import overview_router
# from fastapi.openapi.utils import get_openapi
from Reserve_router import Reserve_router

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


app.openapi = custom_openapi


app.include_router(overview_router)
app.include_router(distribution_router)
app.include_router(Reserve_router)
app.include_router(change_router)
app.include_router(Holder_router)

if __name__ == '__main__':
    uvicorn.run(app,host='45.76.183.129', port='8000')
