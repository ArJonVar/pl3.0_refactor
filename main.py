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

@log_exceptions
def ss_api_calls(scopeObjectId, logr=ghetto_logger('main.py', False)):
    '''does the ss api call once so that row_id_to_row_dict can use the data to extract meaningful meta data'''
    smart = smartsheet.Smartsheet(smartsheet_token)
    smart.errors_as_exceptions(True)
    try: 
        sheet = smart.Sheets.get_sheet(scopeObjectId) 
        return sheet 
    except ApiError:
        error = "APIERROR: failure to find scopeObjectId, this means api key cannot see the sheet webhook is looking at"
        return(error)

@log_exceptions
def row_id_to_row_dict(row_id, sheet, event_type, logr=ghetto_logger('main.py', False)):
    '''pulls data on webhook row id (url, row index) to make it clear what is happening before script runs'''
    logr.log('were in')
    if type(sheet) == smartsheet.models.sheet.Sheet:
        url = sheet.to_dict().get('permalink')
        index = "failed to find row! must not match the scopeObjectID"
        for i, row in enumerate(sheet.to_dict().get('rows')):
            if row.get('id') == row_id:
                index = i + 1
        data = {'row_index': index, "row_id":row_id, 'url': url, 'event_type': event_type}
        return data   
    else:
        return sheet

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
    logr.log(str(payload))

    # Extract the events into a list of dictionaries
    events = []
    for event in payload.events:
        event_dict = event.dict()
        events.append(event_dict)
    
    logr.log(str(events))


    if str(webhook_id) == '8974468551862148':
        '''if the webhook_id is a match, pull ss data from sheet, and then use webhook payload and ss to extract meaningful data (url/row index)
            so the logging can easily help a human see which row of which sheet is triggering'''
        data = []
        rows= []
        sheet_data = ss_api_calls(scopeObjectId)
        for event in events:
            event_type = event.get('eventType')
            row_id = event.get('rowId')
            data.append(row_id_to_row_dict(row_id, sheet_data, event_type))
            if event_type == 'created':
                rows.append(row_id)

        logr.log(f"-- incomming data: {str(data)}")

    else:
        logr.log("webhookId does not match expectation")

    if len(rows) > 0:
        # if row_ids are here, feed them into subprocess that triggers the funcs.py and does the needed actions
        logr.log(f"-- fed into subprocess: {str(rows)}, {str(webhook_id)}, 'pl3_main.py'")
        # command = configure_argz(rows, webhook_id, 'pl3_main.py')
        # p = subprocess.Popen(command, cwd=sdir)

    return{"sucess": "True", "rows": data, 'last_update':"04/07/23"}


