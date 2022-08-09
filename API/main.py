import os
import sqlite3
from typing import Union

from fastapi import FastAPI
from pydantic import BaseModel

import secret

conn = cur = None
app = FastAPI()


class Item(BaseModel):
    name: str
    price: float
    is_offer: Union[bool, None] = None


@app.on_event('startup')
async def startup_event():
    global cur
    global conn
    conn = sqlite3.connect(secret.path_service)
    cur = conn.cursor()
    cur.execute(f'''ATTATCH DATABASE {secret.path_index} AS DBINDEX;''')
    cur.execute(f'''ATTATCH DATABASE {secret.path_core} AS DBINDEX;''')
    cur.execute(f'''ATTATCH DATABASE {secret.path_util} AS DBINDEX;''')
    conn.commit()


@app.get('/')
async def read_root():
    return {'Hello': 'World'}


@app.get('/items/{item_id}')
async def read_item(item_id: int, q: Union[str, None] = None):
    return {'item_id': item_id, 'q': q}


@app.put('/items/{item_id}')
async def update_item(item_id: int, item: Item):
    return {'item_name': item.name, 'item_id': item_id}

