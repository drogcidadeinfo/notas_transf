import os
import glob
import gspread
import json
import time
import re
import logging
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

# config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_latest_file(extension='xls', directory='.'):
    # Get the most recently modified file with a given extension.
    files = glob.glob(os.path.join(directory, f'*.{extension}'))
    if not files:
        logging.warning("No files found with the specified extension.")
        return None
    return max(files, key=os.path.getmtime)

def retry_api_call(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            if hasattr(error, "resp") and error.resp.status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")

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
    
    # df = df[(df['Pendente a'] >= 7)]
    
    # select relevant columns
    df = df[['Nota', 'Controle', 'Emissão', 'Pendente a', 'Entrada', 'Pendente as', 'Fornecedor', 'Filial Destino']]

    df['Emissão'] = df['Emissão'].dt.strftime('%d/%m/%Y')
    df['Entrada'] = df['Entrada'].dt.strftime('%d/%m/%Y')

    # drop unnecessary columns
    df = df.drop(['Entrada', 'Pendente as'], axis=1)

    # Add a column to prioritize rows where 'Pendente a' >= 10
    df['Priority'] = (df['Pendente a'] >= 10).astype(int)  # 1 for >=10, 0 otherwise

    # Sort by priority first, then by 'Filial Destino' and 'Pendente a'
    df = df.sort_values(by=['Priority', 'Filial Destino', 'Pendente a'], ascending=[False, True, False])

    # Clean up: remove the 'Priority' column and restore original formatting
    df = df.drop(columns=['Priority'])
    # df['Pendente a'] = df['Pendente a'].astype(str) + ' dias'
    df['Filial Destino'] = 'F' + df['Filial Destino'].astype(int).astype(str)
    
    # Normalize Fornecedor values like "F01 - MATRIZ - ..." → "F1"
    def simplify_fornecedor(value):
        if pd.isna(value):
            return None
        value = value.strip()

        match = re.match(r"^F0?(\d+)", value)  # captures F01, F1, F001 → group(1) = 1
        if match:
            return f"F{int(match.group(1))}"  # ensures no leading zero
        return value  # fallback if unexpected format

    df['Fornecedor'] = df['Fornecedor'].apply(simplify_fornecedor)

    return df

def apply_red_background_for_pendente(service, sheet_id, sheet_name):
    """
    Highlights rows where 'Pendente a' >= 10
    Clears all background formatting first.
    """

    # Get sheet ID (gridId)
    sheets_metadata = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheet_metadata = next(
        s for s in sheets_metadata['sheets'] if s['properties']['title'] == sheet_name
    )
    grid_id = sheet_metadata['properties']['sheetId']

    requests = []
    
    col_index = 5

    # 1️⃣ CLEAR ALL BACKGROUND COLORS
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": grid_id
            },
            "cell": {
                "userEnteredFormat": {
                    "backgroundColor": {"red": 1, "green": 1, "blue": 1}
                }
            },
            "fields": "userEnteredFormat.backgroundColor"
        }
    })

    # 2️⃣ ADD CONDITIONAL FORMAT: highlight 'Pendente a' >= 10
    requests.append({
        "addConditionalFormatRule": {
            "rule": {
                "ranges": [
                    {
                        "sheetId": grid_id,
                        "startRowIndex": 1,  # entire sheet
                        "startColumnIndex": col_index - 1,
                        "endColumnIndex": col_index
                    }
                ],
                "booleanRule": {
                    "condition": {
                        "type": "NUMBER_GREATER_THAN_EQ",
                        "values": [{"userEnteredValue": "10"}]
                    },
                    "format": {
                        "backgroundColor": {"red": 1, "green": 0.7, "blue": 0.7}
                    }
                }
            },
            "index": 0
        }
    })

    body = {"requests": requests}
    service.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id, body=body
    ).execute()

def load_filial_files(folder="/home/runner/work/notas_transf/notas_transf/downloads"):
    filial_data = {}

    # Load F1–F18 skipping F11
    for i in range(1, 19):
        if i == 11:
            continue  # F11 = F98

        file_path = os.path.join(folder, f"filial{i}.xlsx")
        key = f"F{i}"

        if os.path.exists(file_path):
            try:
                filial_data[key] = pd.read_excel(file_path, dtype=str)
            except Exception as e:
                logging.error(f"Error loading {file_path}: {e}")
        else:
            logging.warning(f"File not found: {file_path}")

    # Load F98
    file_98 = os.path.join(folder, "filial98.xlsx")
    if os.path.exists(file_98):
        try:
            filial_data["F98"] = pd.read_excel(file_98, dtype=str)
        except Exception as e:
            logging.error(f"Error loading filial98.xlsx: {e}")
    else:
        logging.warning("File not found: filial98.xlsx")

    return filial_data

def fill_nota_emissao(df_transf, filial_data):
    df_transf["Emissão Nota"] = ""

    for idx, row in df_transf.iterrows():
        fornecedor = row["Fornecedor"]  # Example "F3"
        nota_raw = row["Nota"]          # Example "9672 - 0"

        # Validate
        if not isinstance(nota_raw, str) or fornecedor not in filial_data:
            continue

        # Extract nota number ("9672")
        nota_num = nota_raw.split("-")[0].strip()

        df_filial = filial_data[fornecedor]

        # Ensure required columns exist
        if not all(col in df_filial.columns for col in ["Unnamed: 5", "Unnamed: 9"]):
            continue

        # Search for matching nota
        match = df_filial[df_filial["Unnamed: 5"].astype(str).str.strip() == nota_num]

        if not match.empty:
            date_value = match.iloc[0]["Unnamed: 9"]

            # Format to dd/mm/yyyy
            try:
                formatted_date = pd.to_datetime(date_value).strftime("%d/%m/%Y")
            except:
                formatted_date = ""

            df_transf.at[idx, "Emissão Nota"] = formatted_date

    return df_transf

def update_worksheet(df, sheet_id, worksheet_name, client):
    df = df.fillna("")
    rows = [df.columns.tolist()] + df.values.tolist()

    try:
        sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
    except Exception as e:
        logging.error(f"Error accessing '{worksheet_name}' worksheet: {e}")
        return

    logging.info(f"Clearing worksheet '{worksheet_name}'...")
    sheet.clear()

    logging.info(f"Updating worksheet '{worksheet_name}'...")
    sheet.update(rows)

    logging.info(f"Worksheet '{worksheet_name}' updated successfully.")
    
def update_google_sheet(df, sheet_id):
    logging.info("Loading Google credentials...")

    creds_env = os.getenv("GGL_CREDENTIALS")
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]

    if creds_env:
        creds = Credentials.from_service_account_info(json.loads(creds_env), scopes=scope)
    else:
        creds = Credentials.from_service_account_file("notas-transf.json", scopes=scope)

    # gspread client
    client = gspread.authorize(creds)

    # Google Sheets API (for formatting)
    service = build("sheets", "v4", credentials=creds)

    logging.info("Splitting DataFrame into 'transf' and 'dist'...")

    # Valid fornecedores: F1..F18 except F11 + F98
    valid_fornecedores = {f"F{i}" for i in range(1, 19) if i != 11}
    valid_fornecedores.add("F98")

    df_transf = df[df["Fornecedor"].isin(valid_fornecedores)].copy()
    df_dist = df[~df["Fornecedor"].isin(valid_fornecedores)].copy()

    df_transf = df_transf.rename(columns={'Emissão': 'Emissão Controle'})

    # Load filial spreadsheets once
    filial_data = load_filial_files(folder="/home/runner/work/notas_transf/notas_transf/downloads")

    # Fill Emissão Nota
    df_transf = fill_nota_emissao(df_transf, filial_data)

    df_transf = df_transf[['Controle', 'Emissão Controle', 'Nota',
                           'Emissão Nota', 'Pendente a',
                           'Fornecedor', 'Filial Destino']]

    # Remove Controle column only from dist
    if "Controle" in df_dist.columns:
        df_dist = df_dist.drop(columns=["Controle"])

    # Sort dist worksheet alphabetically by Filial Destino
    df_dist = df_dist.sort_values(by="Filial Destino", ascending=True)

    # upload data first
    update_worksheet(df_transf, sheet_id, "transf", client)
    update_worksheet(df_dist, sheet_id, "dist", client)

    # apply formatting after 
    apply_red_background_for_pendente(service, sheet_id, "transf")

def main():
    # runs processing pipeline.
    # directory where selenium downloads the file
    sheet_id = os.getenv("sheet_id")
    download_dir = '/home/runner/work/notas_transf/notas_transf'
    
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
        processed_df = process_dataframe(df2_processed)
        
        # update sheets with the processed df
        update_google_sheet(processed_df, sheet_id=sheet_id)
    else:
        logging.warning("No new files to process.")

if __name__ == "__main__":
    main()
    
