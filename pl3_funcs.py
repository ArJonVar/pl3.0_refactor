import sys
import pandas as pd
import smartsheet
import numpy as np
import time
from datetime import datetime
from smartsheet.exceptions import ApiError
from logger import ghetto_logger


try:
    from smartsheet_grid import grid
except ImportError:
    from .smartsheet_grid import grid

import sys

class pl3Updater:
    '''designed usage:
    [variable] = pl3Updater(token='{insert token}')
    [variable].update_per_row()
    
    the rest of the inputs is designed to come through system arguments in this form: 
    arguments = ['{script_name}.py', 'webhook_id:7782278911813508', 'row_id:7625689239054212']
    alternatively, you can just add webhook_id(INT) and row_id(LIST) directly into pl3Updater
    intended behavior is to use the webhook id to find source sheet id from a pl3_webhooks.json, 
    then find the ENUMERATOR and REGION on the sheet to find the destination information from pl3_regional_ids.json
    with all the info, the source row gets reduced by removing columns with symbols likely not to be part of an update
    destination rows get reduced by removing columns with formulas that will not successfully update
    this list of rows gets matched between the source and distination, and what ever is left gets updated
    by looking and column type and giving it the appropriate update    
    '''

    #region raw inputs to processed inputs#
    def __init__(self, token, dev_bool = False, webhook_id='', row_ids=[]):
        raw_now = datetime.now()
        self.now = raw_now.strftime("%m/%d/%Y %H:%M:%S")
        self.dev_bool= dev_bool
        self.token = token
        self.dest_row_id = None
        grid.token=self.token
        self.smart = smartsheet.Smartsheet(access_token=self.token)
        self.smart.errors_as_exceptions(True)
        if webhook_id != '' and row_ids != []:
            self.webhook_id = webhook_id
            self.row_ids = row_ids
        else:
            self.grab_variables_from_sysv()
        self.source_sheet_id = self.json_router_handler(path="pl3_webhooks.json", input_type="webhook_id", input=self.webhook_id, output_type="sheet_id")
        self.source_enum_column_id = self.json_router_handler(path="pl3_webhooks.json", input_type="webhook_id", input=self.webhook_id, output_type="enum_source_column_id")
        self.update_column_name = self.json_router_handler(path="pl3_webhooks.json", input_type="webhook_id", input=self.webhook_id, output_type="update_column_name")
        row_num = "test_row_num"
        self.log=ghetto_logger("pl3_funcs.py", row_num)
    def grab_variables_from_sysv(self):
        if self.dev_bool == True:
            arguments = ['pl3_funcs_dev.py', 'webhook_id:7782278911813508', 'row_id:1767981130114948']
        else:
            arguments = [i for i in sys.argv]
        self.row_ids = [str(i.split("row_id:")[1]) for i in arguments if i.startswith("row_id:")]
        self.webhook_id = [str(i.split("webhook_id:")[1]) for i in arguments if i.startswith("webhook_id:")][0]

    @staticmethod
    def json_id_router(path, input_type=None, input=None, output_type=None):
        df = pd.read_json(path)
        try:
            return df.loc[df[input_type] == int(input)][output_type].values.tolist()[0]
        except:
            return df.loc[df[input_type] == input][output_type].values.tolist()[0]

    def json_router_handler(self, path, input_type=None, input=None, output_type=None):
        try:
            retrieved_item = self.json_id_router(path, input_type, input, output_type)
            return retrieved_item
        except:
            return f"{input_type}: {input} did not yield a matching {output_type} item in {path}.json, check arguments of json_retriever_wrapper()"
    #endregion
    #region generate update row values (enum/region)
    def generate_master_df(self, sheet_id, delete_empty_enums_rows=True):
        try: 
            master_grid = grid(sheet_id)
            master_grid.fetch_content()
            master_grid.df['row_id'] = master_grid.grid_row_ids
            return master_grid.df
        except: 
            return f"failed at generate_master_df w/ sheet_id: {sheet_id}"

    def retrieve_cell_value(self, sheet_id, column_name, row_id):
        try:
            source_df = self.generate_master_df(sheet_id)
            cell_value = source_df.loc[(source_df.row_id == row_id)][column_name]
            return list(cell_value)[0]
        except:
            return f"could not retrieve value from source sheet, verify that column name {column_name} & row_id {row_id} exists"
    #endregion
    #region manage source data
    def manage_source_data(self):
        self.reduce_columns()
        self.generate_reduced_df()
        self.establish_update_dfs()

    def reduce_columns(self):
        self.reduced_columns = grid(self.source_sheet_id)
        self.reduced_columns.reduce_columns('@|=:_')
        self.column_ids_reduced_final = self.reduced_columns.reduced_column_ids
        self.column_names_final = self.reduced_columns.reduced_column_names
        # this is bad use of name space, the name of function matches the name of function in grid, fix!
        if self.webhook_id != '7782278911813508':
            self.column_ids_reduced_final = list(self.reduced_columns.column_df[self.reduced_columns.column_df['title']==self.update_column_name]['id'])
            self.column_names_final = [self.update_column_name]

    def generate_reduced_df(self):
        reduced_sheet = self.smart.Sheets.get_sheet(self.source_sheet_id, row_ids=self.current_row_id, column_ids = self.column_ids_reduced_final, level='2', include='objectValue').to_dict()
        self.reduced_rows = [i.get('cells') for i in reduced_sheet.get('rows')]


    def filter_value_by_type(self, json_key):
        data_by_key=[]
        for row_i in self.reduced_rows:
            row_list = []
            for cell in row_i:
                row_list.append(cell.get(json_key))
            data_by_key.append(row_list)
        return data_by_key

    def establish_update_dfs(self):
        self.updaterow_byval_df = pd.DataFrame(self.filter_value_by_type('value'), columns= self.column_names_final)
        self.updaterow_bydisp_df = pd.DataFrame(self.filter_value_by_type('displayValue'), columns= self.column_names_final)
        self.updaterow_byobj_df = pd.DataFrame(self.filter_value_by_type('objectValue'), columns= self.column_names_final)
    #endregion
    #region manage destination data
    def manage_destination_data(self, dest_sheet_id):
        self.generate_regional_df(dest_sheet_id)
        self.find_row_id_by_value(dest_sheet_id)

    def delete_empty_cells(self, grid, reference_column_name, sheet_id, delete = False):
        empty_value_list = grid.df.index[grid.df[reference_column_name].isnull()].tolist()
        empty_row_ids = [grid.grid_row_ids[i] for i in empty_value_list ]
        if delete == True:
            try:
                self.smart.Sheets.delete_rows(
                sheet_id,                       # sheet_id
                empty_row_ids)     # row_ids
            except:
                pass  

    def generate_regional_df(self, dest_sheet_id):
        destination_grid = grid(dest_sheet_id)
        destination_grid.fetch_content()
        self.delete_empty_cells(destination_grid, "ENUMERATOR", dest_sheet_id)
        destination_column_df= destination_grid.column_df
        try:
            # droping columns with formulas
            self.reduced_dest_col_df = destination_column_df[destination_column_df['formula'].notnull()==False]
        except KeyError:
            self.reduced_dest_col_df = destination_column_df

    def find_row_id_by_value(self, sheet_id):
        try: 
            enum=str(self.enum)
            destination_sheet = self.generate_master_df(sheet_id)
            row_id = destination_sheet.loc[(destination_sheet.ENUMERATOR == enum)]['row_id']
            self.dest_row_id = (list(row_id.values)[0])
        except: 
            self.log.log("destination_row_id could not be found, so no update will be made")
    #endregion
    #region process column matching source // dest
    def process_matching(self):
        self.matching_columns()
        self.establish_matched_update_dfs()
        #defaults to "display value"
        self.main_updaterow_df = self.updaterow_bydisp_df
        self.matched_regional_column_df =self.reduced_dest_col_df[self.reduced_dest_col_df['title'].isin(self.matched_columns)]
        self.sort_into_main_df()
        self.processing_blanks()

    def matching_columns(self):
        self.matched_columns = [i for i in self.reduced_dest_col_df.title.values.tolist() if i in self.updaterow_byval_df.columns]

    def establish_matched_update_dfs(self):
        self.updaterow_byobj_df = self.updaterow_byobj_df[self.matched_columns]   
        self.updaterow_byval_df = self.updaterow_byval_df[self.matched_columns]
        self.updaterow_bydisp_df = self.updaterow_bydisp_df[self.matched_columns]
    
    def sort_into_main_df(self):
        #DEBUGGING BY COLUMN TYPE: self.log.log(self.matched_regional_column_df.type)
        for columntype,columnname in zip(self.matched_regional_column_df.type, self.matched_regional_column_df.title):
            if columntype in ('MULTI_CONTACT_LIST', 'MULTI_PICKLIST', 'CONTACT_LIST'):
                self.main_updaterow_df[columnname] = self.updaterow_byobj_df[columnname]
            if columntype in ('DATE', 'CHECKBOX'):
                self.main_updaterow_df[columnname] = self.updaterow_byval_df[columnname]

    def processing_blanks(self):
        self.main_updaterow_df = self.main_updaterow_df.replace(r'^\s*$', np.nan, regex=True)
        self.main_updaterow_df = self.main_updaterow_df.fillna('')
    #endregion
    #region updete_per_cell
    def update_per_cell(self):
        self.log.log("starting update per cell...")
        update_values = self.main_updaterow_df.values.tolist()
        for row, rownum in zip(update_values, range(len(update_values))):
            new_row = self.smart.models.Row()
            new_row.id = int(self.dest_row_id)
            for item, column, colname in zip(row, self.matched_regional_column_df.id, self.matched_regional_column_df.title):
                self.log.log(f'''
                    COL:  {colname} COLID:{column}
                    Value: {item}
                ''')
                if type(item) == dict:
                    try:
                        for i in item.get('values'):
                            try:
                                del i['imageId']
                            except KeyError:
                                pass
                    except:
                        pass
                    try:
                        if item.get('imageId') != None:
                            try:
                                del item['imageId']
                            except KeyError:
                                pass
                    except:
                        pass                 
                    new_cell = self.smart.models.Cell()
                    new_cell.column_id = column
                    try:
                        new_cell.display_value = item["values"][0]["name"]
                    except:
                        #to accomidate for multi picklist
                        #new_cell.display_value = item["values"]
                        pass
                    new_cell.object_value = item
                    new_cell.strict = False
                    new_cell.override_validation = True
                    new_cell.allowPartialSuccess="true"
                    #append cell to new_row
                    new_row.cells.append(new_cell)
                else:
                    # skipps blank items, which helps with resource allocation
                    if not(item == "" or item == None):
                        new_cell = self.smart.models.Cell()
                        new_cell.column_id = column
                        new_cell.value = item
                        new_cell.strict = False
                        new_cell.override_validation = True
                        new_cell.allowPartialSuccess="true"
                        #append cell to new_row
                        new_row.cells.append(new_cell)
                    else: 
                        self.log.log("skipped")
        try:
            # self.log.log('NEW ROW:', new_row)
            response = self.smart.Sheets.update_rows(self.dest_sheet_id, [new_row])
            # self.log.log(response)
            self.log.log(f"Complete Row Update, {response.message} at {self.now} + 5 hrs - 12 hrs")
        except:
            # region !!NEW DEBUGGING TOOL!! (go through posts one at a time, USE f(X)!!)
            for item, column, colname in zip(row, self.matched_regional_column_df.id, self.matched_regional_column_df.title):
                self.log.log(f'''
                    COL:  {colname} COLID:{column}
                    Value: {item}
                ''')
                if type(item) == dict:
                    try:
                        for i in item.get('values'):
                            try:
                                del i['imageId']
                            except KeyError:
                                pass
                    except:
                        pass
                    try:
                        if item.get('imageId') != None:
                            try:
                                del item['imageId']
                            except KeyError:
                                pass
                    except:
                        pass
                    new_row = self.smart.models.Row()
                    new_row.id = int(self.dest_row_id)                 
                    new_cell = self.smart.models.Cell()
                    new_cell.column_id = column
                    try:
                        new_cell.display_value = item["values"][0]["name"]
                    except:
                        #to accomidate for multi picklist
                        #new_cell.display_value = item["values"]
                        pass
                    new_cell.object_value = item
                    new_cell.strict = False
                    new_cell.override_validation = True
                    new_cell.allowPartialSuccess="true"
                    #append cell to new_row
                    new_row.cells.append(new_cell)
                    response = self.smart.Sheets.update_rows(self.dest_sheet_id, [new_row])
                else:
                    # skipps blank items, which helps with resource allocation
                    if not(item == "" or item == None):
                        new_row = self.smart.models.Row()
                        new_row.id = int(self.dest_row_id)
                        new_cell = self.smart.models.Cell()
                        new_cell.column_id = column
                        new_cell.value = item
                        new_cell.strict = False
                        new_cell.override_validation = True
                        new_cell.allowPartialSuccess="true"
                        #append cell to new_row
                        new_row.cells.append(new_cell)
                        response = self.smart.Sheets.update_rows(self.dest_sheet_id, [new_row])
                    else: 
                        self.log.log("skipped")
# endregion
            self.log.log(f"Error required partial post at {self.now}  + 5 hrs - 12 hrs")
            new_cell.allowPartialSuccess="true"
            response = self.smart.Sheets.update_rows(self.dest_sheet_id, [new_row])
            self.log.log(response)
            self.log.log(f"Partial Success Row updated {response.message}")
        return response
    #endregion

    def update_per_row(self):
        for row_id in self.row_ids:
            row_id = int(row_id)
            # is it bad to make a for loop variable .self, it will keep getting overwritten between each loop...
            self.current_row_id = row_id
            self.enum = self.retrieve_cell_value(self.source_sheet_id, "ENUMERATOR", row_id)
            self.log.log(f"Locating and managing pertinent data for the {self.enum} update")
            self.manage_source_data()
            self.region = self.retrieve_cell_value(self.source_sheet_id,"REGION", row_id)
            self.dest_sheet_id = self.json_router_handler(path="pl3_regional_ids.json", input_type="region", input=self.region, output_type="sheetid")
            self.manage_destination_data(self.dest_sheet_id)
            self.log.log("self.manage_distination_data...")
            self.process_matching()
            self.log.log("self.process_matching...")
            self.update_per_cell()
            self.log.log(f"{self.enum} Updated on the {self.region} Project List")
            time.sleep(12)

