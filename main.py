from fastapi import FastAPI
import json
import subprocess
import os
from pydantic import BaseModel
from typing import List
from logger import ghetto_logger

app = FastAPI()

if os.name == 'nt':
	sdir = r'C:\Egnyte\Private\cobyvardy\Other_Projects\Python\ariel\PL3.0_DO_REFACTOR'
else:
	sdir = r'/home/ariel/pl3.0_refactor'

class Event(BaseModel):
    objectType: str
    eventType: str
    rowId: int
    columnId: int
    userId: int
    timestamp: str

class WebhookPayload(BaseModel):
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

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/pl-update")
async def plupdate(payload: WebhookPayload):
    log=ghetto_logger("main.py")
    print(payload)
    log.log(payload +"\n")
    webhook_id = payload.webhookId

    # Extract the events into a list of dictionaries
    events = []
    for event in payload.events:
        event_dict = event.dict()
        events.append(event_dict)
	
    for event in events:
        if webhook_id == 'NEW WEBHOOK NUMBER':
            rows = [event.get('rowId') for event in events if event.get('eventType') == 'created' ]
        
        if len(rows) > 0:
            command = configure_argz(rows, webhook_id, 'pl3_main.py')
            p = subprocess.Popen(command, cwd=sdir)
    
    return{"sucess": True}

