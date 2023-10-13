import sys
from fastapi_cprofile.profiler import CProfileMiddleware
import fastapi
from fastapi import APIRouter
from fastapi.openapi.utils import get_openapi
from fastapi import FastAPI,Response
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from dotenv.main import load_dotenv
import os
import re
from typing import List
from pathlib import Path
import pandas as pd
import plotly.express as px
import numpy as np
from numerize import numerize
import datetime
import _datetime
from sqlalchemy import create_engine
import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objects as go
pd.options.mode.chained_assignment = None
load_dotenv()
