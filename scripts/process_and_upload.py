import os
import glob
import gspread
import json
import math
import time
import re
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font
from openpyxl.styles import PatternFill
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

# constants for magic strings
FILENAME_EXTENSION = 'xls'
FORNECEDOR_KEY = 'Fornecedor:'
COLUMN_NAMES = {
    'Filial': 'Filial Destino',
    'Núm. Contrl.': 'Controle'
}

def get_latest_file(extension='xls', directory='/home/runner/work/notas_transf/notas_transf/'):
    """get latest file with the given extension from the specified directory"""
    # list all files in the directory with the specified extension
    list_of_files = glob.glob(os.path.join(directory, f'*.{extension}'))
    
    if not list_of_files:  
        logging.warning("No files found with the specified extension.")
        return None
    
    # return most recently modified file
    return max(list_of_files, key=os.path.getmtime) 

def retry_api_call(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            if error.resp.status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise error
    raise HttpError("Max retries reached.")

def process_dataframe_2(df2):
    df2 = df2.iloc[:, 1:6]
    df2 = df2[~df2.iloc[:, 0].str.contains('Filial:|Total:|Total Filial:|Total Geral:', na=False)]
    return df2

def process_dataframe(df):
    fornecedores = []
    fornecedor_atual = None

    for index, row in df.iterrows():
        if row.iloc[0] == 'Fornecedor:':
            fornecedor_atual = row.iloc[2]  
        else:
            if fornecedor_atual is not None:
                fornecedores.append(fornecedor_atual)
            else:
                fornecedores.append(None)  # ensure a placeholder is added if no current supplier

    while len(fornecedores) < len(df):  # ensure the list is the same length as the dataframe
        fornecedores.append(None)

    # remove rows that had "Fornecedor:"
    df = df[df.iloc[:, 0] != 'Fornecedor:']

    # ensure lengths match before assignment
    if len(fornecedores) != len(df):
        print(f"Length of fornecedores: {len(fornecedores)}, Length of DataFrame: {len(df)}")

    # assign fornecedores column
    df.loc[:, 'Fornecedor'] = fornecedores[:len(df)]

    df = df.rename(columns={'Filial': 'Filial Destino', 'Núm. Contrl.': 'Controle'})
    
    df['Emissão'] = pd.to_datetime(df['Emissão'], errors='coerce')
    df['Entrada'] = pd.to_datetime(df['Entrada'], errors='coerce')

    df = df[df['Filial Destino'] != 98]
    df = df.dropna(subset=['Emissão', 'Entrada'])

    current_date = datetime.now()
    df['Pendente a'] = (current_date - df['Emissão']).dt.days
    df['Pendente as'] = (current_date - df['Entrada']).dt.days
    """
    df['Pendente a'] = df['Pendente a'].astype(str) + ' dias'
    df = df[(df['Pendente a'].str.replace(' dias', '').astype(int) >= 7)]
    """
    df = df[(df['Pendente a'] >= 7)]
    
    # select relevant columns
    df = df[['Nota', 'Controle', 'Emissão', 'Pendente a', 'Entrada', 'Pendente as', 'Fornecedor', 'Filial Destino']]

    df['Emissão'] = df['Emissão'].dt.strftime('%d/%m/%Y')
    df['Entrada'] = df['Entrada'].dt.strftime('%d/%m/%Y')

    # drop unnecessary columns
    df = df.drop(['Entrada', 'Pendente as'], axis=1)

    """
    df['Pendente a'] = df['Pendente a'].str.replace(' dias', '').astype(int)
    df = df.sort_values(by=['Filial Destino', 'Pendente a'], ascending=[True, False])
    df['Pendente a'] = df['Pendente a'].astype(str) + ' dias'
    df['Filial Destino'] = 'F' + df['Filial Destino'].astype(int).astype(str)
    """
    # Remove the " dias" suffix and convert 'Pendente a' to integers
    # df['Pendente a'] = df['Pendente a'].str.replace(' dias', '').astype(int)

    # Add a column to prioritize rows where 'Pendente a' >= 10
    df['Priority'] = (df['Pendente a'] >= 10).astype(int)  # 1 for >=10, 0 otherwise

    # Sort by priority first, then by 'Filial Destino' and 'Pendente a'
    df = df.sort_values(by=['Priority', 'Filial Destino', 'Pendente a'], ascending=[False, True, False])

    # Clean up: remove the 'Priority' column and restore original formatting
    df = df.drop(columns=['Priority'])
    # df['Pendente a'] = df['Pendente a'].astype(str) + ' dias'
    df['Filial Destino'] = 'F' + df['Filial Destino'].astype(int).astype(str)
    df["Justificativa"] = ""  # Add a blank column for Justificativa

    # calculate column widths based on data and header lengths
    column_widths = {col: math.ceil(max(df[col].astype(str).map(len).max(), len(col)) * 10) for col in df.columns}

    return df, column_widths

def apply_sheet_formatting(spreadsheet_id, sheet_name, df, column_widths, creds):
    try:
        service = build("sheets", "v4", credentials=creds)
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

        # log all available sheet names for verification
        available_sheets = [sheet["properties"]["title"] for sheet in spreadsheet["sheets"]]
        logging.info(f"Available sheets: {available_sheets}")

        target_sheet_name = "info"  # change to the actual name of sheet
        sheet_id = next(
            sheet["properties"]["sheetId"] for sheet in spreadsheet["sheets"]
            if sheet["properties"]["title"] == target_sheet_name
        )

        # Clear formatting and cell content
        requests = [{
            "updateCells": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": 0,
                    "startColumnIndex": 0,
                    "endRowIndex": len(df),
                    "endColumnIndex": len(df.columns),
                },
                "fields": "userEnteredFormat.backgroundColor, userEnteredFormat.textFormat",
            }
        }]

        # Conditional formatting logic to apply red background
        for index, row in enumerate(df.itertuples(index=False), start=1):  # Iterate through DataFrame rows
            value = str(row[3])  # Assuming column D is at index 3 (0-based)
            match = re.search(r'\d+', value)
            if match:
                number = int(match.group())
                if number >= 10:
                    requests.append({
                        "repeatCell": {
                            "range": {
                                "sheetId": sheet_id,
                                "startRowIndex": index,
                                "endRowIndex": index + 1,
                                "startColumnIndex": 3,  # Column D (0-based index)
                                "endColumnIndex": 4
                            },
                            "cell": {
                                "userEnteredFormat": {
                                    "backgroundColor": {
                                        "red": 1,
                                        "green": 0,
                                        "blue": 0,
                                        "alpha": 1
                                    }
                                }
                            },
                            "fields": "userEnteredFormat.backgroundColor"
                        }
                    })

        # Add additional requests for header formatting, column widths, etc.
        requests.extend([
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": 0,
                        "endRowIndex": 1
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "textFormat": {"bold": True},
                            "horizontalAlignment": "CENTER"
                        }
                    },
                    "fields": "userEnteredFormat(textFormat,horizontalAlignment)"
                }
            },
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 0,
                            "startColumnIndex": 0,
                            "endColumnIndex": len(df.columns)
                        }
                    }
                }
            }
        ])

        # Add column width adjustment requests dynamically using column_widths
        for i, (col, width) in enumerate(column_widths.items()):
            requests.append({
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": i,
                        "endIndex": i + 1
                    },
                    "properties": {
                        "pixelSize": width
                    },
                    "fields": "pixelSize"
                }
            })

        # Execute batch update for all formatting requests
        service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()

        logging.info("Formatting and sorting applied successfully.")

        # Apply protection after uploading the data
        protect_columns_except(
            df=df,
            spreadsheet_id=spreadsheet_id,
            sheet_id=sheet_id,
            editable_column="Justificativa",
            creds=creds,
        )
        
        set_sharing_permissions(spreadsheet_id, creds)

    except StopIteration:
        logging.error(f"Sheet '{sheet_name}' not found in the spreadsheet.")
    except HttpError as error:
        logging.error(f"An error occurred: {error}")

def protect_columns_except(df, spreadsheet_id, sheet_id, editable_column, creds):
    try:
        service = build("sheets", "v4", credentials=creds)

        # Get the index of the editable column
        editable_index = df.columns.tolist().index(editable_column)
        logging.info(f"Column '{editable_column}' found.")

        # Create a request to protect all columns except the editable one
        requests = []

        # Protect all columns before the editable column
        if editable_index > 0:
            requests.append({
                "addProtectedRange": {
                    "protectedRange": {
                        "range": {
                            "sheetId": sheet_id,
                            "startColumnIndex": 0,
                            "endColumnIndex": editable_index,
                        },
                        "description": "Protect all columns except 'Justificativa'",
                        "editors": {
                            "users": ["informatica@drogcidade.com.br", "notas-transf@notas-transf.iam.gserviceaccount.com", "adm@drogcidade.com.br"]  # Add other users explicitly if needed
                        },
                        "warningOnly": False
                    }
                }
            })

        # Protect all columns after the editable column
        if editable_index + 1 < len(df.columns):
            requests.append({
                "addProtectedRange": {
                    "protectedRange": {
                        "range": {
                            "sheetId": sheet_id,
                            "startColumnIndex": editable_index + 1,
                            "endColumnIndex": len(df.columns),
                        },
                        "description": "Protect all columns except 'Justificativa'",
                        "editors": {
                            "users": ["informatica@drogcidade.com.br", "notas-transf@notas-transf.iam.gserviceaccount.com", "adm@drogcidade.com.br"]  # Add other users explicitly if needed
                        },
                        "warningOnly": False
                    }
                }
            })
        
        # Set the width of the unprotected column
        requests.append({
            "updateDimensionProperties": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "startIndex": 6,
                    "endIndex": 6 + 1
                },
                "properties": {
                    "pixelSize": 500  # Adjust the pixel size as needed
                },
                "fields": "pixelSize"
            }
        })

        # Execute the batch update request
        if requests:
            body = {"requests": requests}
            service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=body
            ).execute()

        logging.info("Columns protected successfully.")

    except HttpError as error:
        logging.error(f"Failed to protect columns: {error}")

def set_sharing_permissions(spreadsheet_id, creds):
    """
    Set the spreadsheet to allow link sharing with edit rights.
    """
    from googleapiclient.discovery import build

    try:
        drive_service = build("drive", "v3", credentials=creds)
        permission = {
            "type": "anyone",
            "role": "writer",  # Allows edit access
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission,
        ).execute()
        logging.info("Permissions updated: Anyone with the link can edit.")
    except HttpError as error:
        logging.error(f"Failed to update sharing permissions: {error}")

def revoke_edit_permissions(spreadsheet_id, creds):
    """
    Revoke edit permissions for anyone with the link.
    """
    from googleapiclient.discovery import build

    try:
        drive_service = build("drive", "v3", credentials=creds)
        permission = {
            "type": "anyone",
            "role": "reader",  # Change role to viewer
        }
        drive_service.permissions().create(
            fileId=spreadsheet_id,
            body=permission,
        ).execute()
        logging.info("Permissions updated: Link-sharing set to view-only.")
    except HttpError as error:
        logging.error(f"Failed to revoke edit permissions: {error}")

def update_google_sheet(df, sheet_name, column_widths):
    # authenticate using credentials from the environment (json file is gitsecret)
    logging.info("Checking Google credentials environment variable...")
    creds_json = os.getenv("GGL_CREDENTIALS") 
    if creds_json is None:
        logging.error("Google credentials not found in environment variables.")
        return

    # parse JSON credentials and authenticate with sheets api
    creds_dict = json.loads(creds_json)
    logging.info("Attempting to authenticate with Google Sheets API...")
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)

    # open file on google sheets and select the worksheet by name
    sheet = client.open(sheet_name).worksheet("info")
    spreadsheet_id = client.open(sheet_name).id  # get spreadsheet ID

    # prep the data from the processed df
    logging.info("Processing DataFrame for Google Sheets update...")
    df = df.fillna("")  # replace NaN with empty strings
    rows = [df.columns.tolist()] + df.values.tolist()

    # clear existing data in the sheet before updating
    logging.info("Clearing existing data in the Google Sheet...")
    sheet.clear()

    # update sheets with the new data
    logging.info("Updating Google Sheet with processed data...")
    retry_api_call(lambda: sheet.update(rows))

    # apply formatting to the the sheet
    logging.info("Applying formatting to Google Sheet...")
    apply_sheet_formatting(spreadsheet_id, "info", df, column_widths, creds)

    logging.info("Google Sheet updated successfully.")

def main():
    """runs processing pipeline."""
    # directory where selenium downloads the file
    download_dir = '/home/runner/work/notas_transf/notas_transf/'
    
    # get latest file from specified download directory
    latest_xls_file = get_latest_file(directory=download_dir)
    
    if latest_xls_file:
        logging.info(f"Loaded file: {latest_xls_file}")
        
        try:
            df2 = pd.read_excel(latest_xls_file, skiprows=2)
        except Exception as e:
            logging.error(f"Error loading the file: {e}")
            return
        
        # process initialdf and obtain column widths
        df2_processed = process_dataframe_2(df2)
        processed_df, column_widths = process_dataframe(df2_processed)
        
        # update sheets with the processed df
        update_google_sheet(processed_df, sheet_name="notas_transf", column_widths=column_widths)
    else:
        logging.warning("No new files to process.")

if __name__ == "__main__":
    main()
