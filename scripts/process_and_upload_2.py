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

# Config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_all_files(directory='.', extensions=('xls', 'xlsx')):
    """Return list of all files with given extensions in directory, sorted by modification time (oldest first)."""
    files = []
    for ext in extensions:
        files.extend(glob.glob(os.path.join(directory, f'*.{ext}')))
    if not files:
        return []
    return sorted(files, key=os.path.getmtime)

def retry_api_call(func, retries=3, delay=2):
    """Retry a Google API call on HTTP 500 errors."""
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

def clean_transfer_file(file_path):
    """
    Clean one transfer file using the logic from the Jupyter notebook,
    then add pending days and sort.
    """
    logging.info(f"Processing {file_path} ...")

    # 1. Load with skiprows=2
    df = pd.read_excel(file_path, skiprows=2, header=0)

    # 2. Keep only columns 1:36 (as in Jupyter)
    df = df.iloc[:, 1:36]

    # 3. Extract Filial Destino and Fornecedor from header rows
    filiais_destino = []
    fornecedores = []
    filial_atual = None
    fornecedor_atual = None

    for idx, row in df.iterrows():
        first_val = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        if 'Filial:' in first_val:
            filial_atual = row.iloc[1]          # value is in the second column
            logging.debug(f"Row {idx}: Found Filial -> {filial_atual}")
        elif 'Fornecedor:' in first_val:
            fornecedor_atual = row.iloc[2]       # value is in the third column
            logging.debug(f"Row {idx}: Found Fornecedor -> {fornecedor_atual}")
        else:
            filiais_destino.append(filial_atual)
            fornecedores.append(fornecedor_atual)

    # 4. Remove header rows (where first column contains 'Filial:' or 'Fornecedor:')
    mask = ~df.iloc[:, 0].astype(str).str.contains('Filial:|Fornecedor:', na=False, regex=True)
    clean_df = df[mask].copy()

    # Ensure we have the same number of extracted values
    # (if the file ended with a header row, we may have extra placeholders – truncate)
    min_len = min(len(clean_df), len(filiais_destino), len(fornecedores))
    clean_df = clean_df.iloc[:min_len].copy()
    clean_df['Filial Destino'] = filiais_destino[:min_len]
    clean_df['Fornecedor'] = fornecedores[:min_len]

    # 5. Remove rows that are totals
    clean_df = clean_df[~clean_df.iloc[:, 0].astype(str).str.contains('Total:|Total Geral:', na=False, regex=True)]

    # 6. Drop rows where control number is missing
    clean_df = clean_df.dropna(subset=[clean_df.columns[3]])  # Núm. Contrl. is at index 3 after slicing

    # 7. Rename the first six columns (as per Jupyter)
    #    Columns after slicing: index0..index5 are the meaningful transaction columns
    clean_df = clean_df.rename(columns={
        clean_df.columns[0]: 'Nota',
        clean_df.columns[1]: 'Filial (data)',       # temporary, will not be used
        clean_df.columns[2]: 'Emissão',
        clean_df.columns[3]: 'Núm. Contrl.',
        clean_df.columns[4]: 'Valor Total',
        clean_df.columns[5]: 'Filial Origem',
    })

    # 8. Keep only the columns we need (drop 'Nota' and the temporary 'Filial (data)')
    clean_df = clean_df[['Filial Origem', 'Emissão', 'Núm. Contrl.', 'Valor Total', 'Filial Destino', 'Fornecedor']]

    # 9. Convert Emissão to datetime and compute pending days
    clean_df['Emissão'] = pd.to_datetime(clean_df['Emissão'], errors='coerce')
    clean_df = clean_df.dropna(subset=['Emissão'])
    current_date = datetime.now()
    clean_df['Pendente a'] = (current_date - clean_df['Emissão']).dt.days

    # 10. Format date as dd/mm/YYYY
    clean_df['Emissão'] = clean_df['Emissão'].dt.strftime('%d/%m/%Y')

    # 11. Simplify Fornecedor (e.g. "F01 - MATRIZ" → "F1")
    def simplify_fornecedor(val):
        if pd.isna(val):
            return None
        val = str(val).strip()
        match = re.match(r"^F0?(\d+)", val)
        if match:
            return f"F{int(match.group(1))}"
        return val
    clean_df['Fornecedor'] = clean_df['Fornecedor'].apply(simplify_fornecedor)

    # 12. Sort: first by pending days (highest first), then by Filial Destino
    clean_df = clean_df.sort_values(by=['Pendente a', 'Filial Destino'], ascending=[False, True])

    logging.info(f"Finished cleaning {file_path}: {len(clean_df)} rows")
    return clean_df

def update_worksheet(df, sheet_id, worksheet_name, client):
    """Replace the content of a worksheet with the given dataframe."""
    try:
        sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        sh = client.open_by_key(sheet_id)
        sheet = sh.add_worksheet(title=worksheet_name, rows=1000, cols=len(df.columns))
        logging.info(f"Worksheet '{worksheet_name}' created.")

    # Clear existing content
    sheet.clear()

    if df.empty:
        logging.warning("DataFrame is empty. Nothing to upload.")
        return

    # Convert any non‑serializable values to strings or empty strings
    def make_serializable(val):
        # Handle NaN/None
        if pd.isna(val):
            return ''
        
        # Handle pandas Timestamp / datetime
        if hasattr(val, 'strftime'):
            try:
                return val.strftime('%d/%m/%Y')  # Use the same format as before
            except (ValueError, AttributeError):
                return ''
        
        # Handle numpy datetime64
        if hasattr(val, 'dtype') and 'datetime' in str(val.dtype):
            try:
                return pd.Timestamp(val).strftime('%d/%m/%Y')
            except (ValueError, TypeError):
                return ''
        
        # Handle float values (check for NaN again just in case)
        if isinstance(val, float):
            if pd.isna(val) or val != val:  # NaN check
                return ''
        
        return val

    # Build data rows with conversion
    data = []
    # headers
    data.append(df.columns.tolist())
    # rows
    for row in df.values:
        converted_row = [make_serializable(cell) for cell in row]
        data.append(converted_row)

    # Upload in chunks if data is large (Google Sheets has limits)
    chunk_size = 5000  # Adjust based on your needs
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        start_row = i + 1  # +1 because sheets are 1-indexed and we have headers
        range_name = f'A{start_row}' if i == 0 else f'A{start_row}'
        
        try:
            if i == 0:
                # First chunk includes headers
                sheet.update(range_name='A1', values=chunk)
            else:
                # Subsequent chunks are data only
                sheet.update(range_name=range_name, values=chunk)
            
            logging.info(f"Uploaded chunk {i//chunk_size + 1}/{(len(data)-1)//chunk_size + 1}")
        except Exception as e:
            logging.error(f"Error uploading chunk: {e}")
            raise

    # Format header row bold
    try:
        service = build('sheets', 'v4', credentials=client.auth)
        requests = [{
            'repeatCell': {
                'range': {
                    'sheetId': sheet.id,
                    'startRowIndex': 0,
                    'endRowIndex': 1
                },
                'cell': {
                    'userEnteredFormat': {
                        'textFormat': {'bold': True}
                    }
                },
                'fields': 'userEnteredFormat.textFormat.bold'
            }
        }]
        body = {'requests': requests}
        service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=body).execute()
    except Exception as e:
        logging.warning(f"Could not format header row: {e}")

    logging.info(f"Uploaded {len(df)} rows to '{worksheet_name}'.")

def update_google_sheet(df, sheet_id):
    """Authorize and update the Google Sheet."""
    logging.info("Loading Google credentials...")

    creds_env = os.getenv("GGL_CREDENTIALS")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_env:
        creds = Credentials.from_service_account_info(
            json.loads(creds_env), scopes=scope
        )
    else:
        creds = Credentials.from_service_account_file(
            "notas-transf.json", scopes=scope
        )

    client = gspread.authorize(creds)

    # Update the worksheet named "transf_total"
    update_worksheet(df, sheet_id, "transf_total", client)

def main():
    # Get target sheet ID from environment variable
    sheet_id = os.getenv("sheet_id")
    if not sheet_id:
        logging.error("Environment variable 'sheet_id' not set.")
        return

    download_dir = '/home/runner/work/notas_transf/notas_transf'   # adjust as needed

    # Get all Excel files in the directory
    all_files = get_all_files(directory=download_dir, extensions=('xls', 'xlsx'))
    if not all_files:
        logging.warning("No Excel files found in the directory.")
        return

    logging.info(f"Found {len(all_files)} file(s) to process.")

    # Process each file and collect dataframes
    dataframes = []
    for file_path in all_files:
        try:
            df_cleaned = clean_transfer_file(file_path)
            if not df_cleaned.empty:
                dataframes.append(df_cleaned)
        except Exception as e:
            logging.error(f"Error processing {file_path}: {e}")
            continue

    if not dataframes:
        logging.error("No data could be extracted from any file.")
        return

    # Combine all
    final_df = pd.concat(dataframes, ignore_index=True)
    logging.info(f"Total combined rows: {len(final_df)}")

    # Upload to Google Sheets
    update_google_sheet(final_df, sheet_id)

if __name__ == "__main__":
    main()
