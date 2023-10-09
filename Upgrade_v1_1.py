from fastapi import APIRouter
from imports import *

import pandas as pd


upgrade_v1_1_router = APIRouter(
    prefix='/Upgrade',
    tags=['v1.1'],

)

