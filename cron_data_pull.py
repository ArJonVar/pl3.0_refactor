from logger import ghetto_logger
import smartsheet
from smartsheet.exceptions import ApiError
from globals import smartsheet_token
import json
import os

'''cron pull so that the main.py logger can include row data without having to make smartsheet call'''

def log_exceptions(func, logr=ghetto_logger('cron_data_pull.py', False)):
    '''decorator to catch and log errors in main .txt (you can also go to venv/bin/gunicorn_erroroutput.txt for full error)'''
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logr.log(F"ERROR in {func.__name__}(): {e}")
            raise e
    return wrapper

@log_exceptions
def ss_api_calls(scopeObjectId, logr=ghetto_logger('cron_data_pull.py', False)):
    '''does the ss api call once so that row_id_to_row_dict can use the data to extract meaningful meta data'''
    smart = smartsheet.Smartsheet(smartsheet_token)
    smart.errors_as_exceptions(True)
    try: 
        sheet = smart.Sheets.get_sheet(scopeObjectId) 
        return sheet.to_dict()
    except ApiError:
        error = "APIERROR: failure to find scopeObjectId, this means api key cannot see the sheet webhook is looking at"
        return(error)

@log_exceptions
def overwrite_json(data, file_path):
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=4)

if __name__ == "__main__":
    if os.name == 'nt':
        path = r'C:\Egnyte\Private\cobyvardy\Other_Projects\Python\ariel\PL3.0_DO_REFACTOR\smartsheet_pull.json'
    else:
        path = r'/home/ariel/pl3.0_refactor/smartsheet_pull.json'

    sheet = ss_api_calls(8025857521411972)
    overwrite_json(sheet, path)