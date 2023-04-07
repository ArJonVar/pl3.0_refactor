from fastapi import FastAPI
import json
import subprocess
import os
from pydantic import BaseModel
from typing import List
from logger import ghetto_logger
from smartsheet_grid import grid
from globals import smartsheet_token

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "updated on 04.06.2023"}

if os.name == 'nt':
	sdir = r'C:\Egnyte\Private\cobyvardy\Other_Projects\Python\ariel\PL3.0_DO_REFACTOR'
else:
	sdir = r'/home/ariel/pl3.0_refactor'

class Event(BaseModel):
    '''defines shape of the events list in WebhookPayload'''
    objectType: str
    eventType: str
    rowId: int
    columnId: int
    userId: int
    timestamp: str

class WebhookPayload(BaseModel):
    '''defines the shape of the expected body'''
    nonce: str
    timestamp: str
    webhookId: int
    scope: str
    scopeObjectId: int
    events: List[Event]

def configure_argz(rows_input, webhook_id_input, script):
    '''takes inputs and runs the python script that makes the changes as a subprocess'''
    command = ['python', script]
    command.append(f"webhook_id:{webhook_id_input}")
    for row in rows_input:
        row = f'row_id:{row}'
        command.append(row)
    print('command', command)
    return command 

def row_id_to_row_dict(row_id, scopeObjectId):
    '''pulls data on webhook row id (url, row index) to make it clear what is happening before script runs'''
    grid.token=smartsheet_token
    sheet = grid(scopeObjectId)
    sheet.fetch_content()
    index = sheet.df.loc[sheet.df['id']== row_id].index[0] +1
    url = sheet.grid_url
    return {'index': index, 'url': url}


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/pl-update")
async def plupdate(payload: WebhookPayload):
    logr=ghetto_logger("main.py")
    webhook_id = payload.webhookId
    scopeObjectId=payload.scopeObjectId
    logr.log(str(payload))

    # Extract the events into a list of dictionaries
    events = []
    for event in payload.events:
        event_dict = event.dict()
        events.append(event_dict)
    
    logr.log(str(events))
        
    if str(webhook_id) == '7589161210275716':
        logr.log("1")
        rows = [event.get('rowId') for event in events if event.get('eventType') == 'created' ]
        [logr.log(str(row_id_to_row_dict(row))) for row in rows]
         
    else:
        logr.log("webhook not viable")
        rows = []


    if len(rows) > 0:
        logr.log("3")
        logr.log(f"{str(rows)}, {str(webhook_id)}, 'pl3_main.py'")
        # command = configure_argz(rows, webhook_id, 'pl3_main.py')
        # p = subprocess.Popen(command, cwd=sdir)
    else:
        rows = ["no event rows w/ eventType == 'created'"]


    return{"sucess": "True", "rows": rows, 'last_update':"04/07/23"}
    # return {"message":"04/06/23", "test": webhook_id}

# DEBUGGING:
# @app.get("/")
# async def root():
#     return {"message": "Hello World"}

# @app.post("/pl-update")
# async def plupdate():
#     return {"message":"04/06/23"}
