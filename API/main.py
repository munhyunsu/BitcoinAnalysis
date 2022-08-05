import os
import sqlite3
from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

import secret

app = FastAPI()


@app.get('/')
async def read_root():
    return {'Hello': 'World'}


@app.get('/items/{item_id}')
async def read_item(item_id: int, q: Union[str, None] = None):
    return {'item_id': item_id, 'q': q}

