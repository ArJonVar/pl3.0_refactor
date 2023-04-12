from fastapi import FastAPI
import json
import subprocess
import os
from pydantic import BaseModel
from typing import List
from logger import ghetto_logger
import smartsheet
from smartsheet.exceptions import ApiError
from globals import smartsheet_token

app = FastAPI()

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

def log_exceptions(func, logr=ghetto_logger('main.py', False)):
    '''decorator to catch and log errors in main .txt (you can also go to venv/bin/gunicorn_erroroutput.txt for full error)'''
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logr.log(F"ERROR in {func.__name__}(): {e}")
            raise e
    return wrapper

if os.name == 'nt':
	sdir = r'C:\Egnyte\Private\cobyvardy\Other_Projects\Python\ariel\PL3.0_DO_REFACTOR'
else:
	sdir = r'/home/ariel/pl3.0_refactor'

@log_exceptions
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
    return {"message": "updated on 04.06.2023"}

@log_exceptions
@app.post("/pl-update")
async def plupdate(payload: WebhookPayload):
    '''we take in a webhook payload from a post request, and use it to trigger the python script that will make nessisary updates and logs'''
    logr=ghetto_logger("main.py")
    webhook_id = payload.webhookId
    scopeObjectId=payload.scopeObjectId
    logr.log(f"PAYLOAD: {str(payload)}")

    # Extract the events into a list of dictionaries
    events = []
    for event in payload.events:
        event_dict = event.dict()
        events.append(event_dict)

    if str(webhook_id) == '8974468551862148':
        '''if the webhook_id is a match, pull ss data from sheet, and then use webhook payload and ss to extract meaningful data (url/row index)
            so the logging can easily help a human see which row of which sheet is triggering'''
        rows= []
        for event in events:
            if event.get('eventType') == 'created':
                rows.append(event.get('rowId'))

    else:
        logr.log("webhookId does not match expectation")

    if len(rows) > 0:
        # if row_ids are here, feed them into subprocess that triggers the funcs.py and does the needed actions
        logr.log(f"-- fed into subprocess: {str(rows)}, {str(webhook_id)}, 'pl3_main.py'")
        command = configure_argz(rows, webhook_id, 'pl3_main.py')
        p = subprocess.Popen(command, cwd=sdir)

    return{"sucess": "True", "rows": rows, 'last_update':"04/07/23"}


