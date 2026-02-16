import os
import glob
import json
import time
import logging
import pandas as pd
import gspread

from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

# -------------------------------------------------
# Config logging
# -------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -------------------------------------------------
# File utils
# -------------------------------------------------
def get_all_files(directory=".", extensions=("xls", "xlsx")):
    """Return list of all files with given extensions in directory, sorted by modification time (oldest first)."""
    files = []
    for ext in extensions:
        files.extend(glob.glob(os.path.join(directory, f"*.{ext}")))
    return sorted(files, key=os.path.getmtime) if files else []


# -------------------------------------------------
# Google API retry helper
# -------------------------------------------------
def retry_api_call(func, retries=3, delay=2):
    """Retry a Google API call on HTTP 500 errors."""
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            status = getattr(getattr(error, "resp", None), "status", None)
            if status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")


# -------------------------------------------------
# Your notebook logic, packaged into a function
# -------------------------------------------------
DROP_COLS = [
    "Entrada", "Desc.\nItens", "Valor Líquido", "Desc. Arred.", "Unnamed: 9",
    "  Acrés. Arred.", "Unnamed: 11", "Frete", "Unnamed: 13", "D.A.+\nSeguro",
    "Bas.Icms\nRetido", "Vlr.Icms\nRetido", "Unnamed: 17", "Repas.\nIcms",
    "Unnamed: 19", "Bas.Icms\nNormal", "Vlr.Icms\nNormal", "Unnamed: 22",
    "Conf", "Extra Nota", "Unnamed: 25", "Total Nota", "Total + Extra Nota",
]

def clean_transfer_file(file_path: str) -> pd.DataFrame:
    """
    Load one .xls/.xlsx and produce the clean dataframe:
    ['Filial Origem', 'Filial Destino', 'Emissão', 'Núm. Contrl.', 'Valor Total']
    """
    logging.info(f"Reading: {os.path.basename(file_path)}")

    # Same as notebook
    df = pd.read_excel(file_path, skiprows=2, header=0)

    # Select specific columns (2nd to 36th in 1-based indexing)
    df = df.iloc[:, 1:36]

    # Drop unnecessary columns (ignore missing)
    df = df.drop(columns=DROP_COLS, errors="ignore")

    # Rename the first 5 columns to match your notebook intent
    # (This assumes those columns exist after drop; if not, it'll error clearly.)
    df = df.rename(
        columns={
            df.columns[0]: "Nota",
            df.columns[1]: "Filial",
            df.columns[2]: "Emissão",
            df.columns[3]: "Núm. Contrl.",
            df.columns[4]: "Valor Total",
        }
    )

    # Extract Filial and Fornecedor markers
    filiais = []
    fornecedores = []
    filial_atual = None
    fornecedor_atual = None

    for _, row in df.iterrows():
        nota_value = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""

        if "Filial:" in nota_value:
            filial_atual = row.iloc[1]
        elif "Fornecedor:" in nota_value:
            fornecedor_atual = row.iloc[2]
        else:
            filiais.append(filial_atual)
            fornecedores.append(fornecedor_atual)

    # Remove marker rows and assign extracted values
    mask = ~df["Nota"].astype(str).str.contains(r"Filial:|Fornecedor:", na=False, regex=True)
    clean_df = df.loc[mask].copy()

    clean_df["Filial"] = filiais[: len(clean_df)]
    clean_df["Fornecedor"] = fornecedores[: len(clean_df)]

    # Remove total rows + require Núm. Contrl.
    clean_df = clean_df[~clean_df["Nota"].astype(str).str.contains(r"Total:|Total Geral:", na=False, regex=True)]
    clean_df = clean_df.dropna(subset=["Núm. Contrl."])

    # Rename columns to your final meaning
    # Note: your notebook maps:
    #   column 1 -> Filial Destino
    #   column 5 -> Filial Origem
    # (Fornecedor is present but not used in final; we keep your final output exactly)
    clean_df = clean_df.rename(
        columns={
            clean_df.columns[0]: "Nota",
            clean_df.columns[1]: "Filial Destino",
            clean_df.columns[2]: "Emissão",
            clean_df.columns[3]: "Núm. Contrl.",
            clean_df.columns[4]: "Valor Total",
            clean_df.columns[5]: "Filial Origem",
        }
    )

    # Drop Nota like notebook
    if "Nota" in clean_df.columns:
        clean_df = clean_df.drop(columns=["Nota"])

    # Order columns
    ordem_colunas = ["Filial Origem", "Filial Destino", "Emissão", "Núm. Contrl.", "Valor Total"]
    clean_df = clean_df[ordem_colunas]

    # Format date
    clean_df["Emissão"] = pd.to_datetime(clean_df["Emissão"], errors="coerce").dt.strftime("%d/%m/%Y")

    # Basic cleanup
    clean_df = clean_df.dropna(subset=["Emissão", "Núm. Contrl."])

    return clean_df


# -------------------------------------------------
# Google Sheets update
# -------------------------------------------------
def update_worksheet(df: pd.DataFrame, sheet_id: str, worksheet_name: str, client: gspread.Client):
    """Replace the content of a worksheet with the given dataframe."""
    sh = client.open_by_key(sheet_id)

    try:
        ws = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=worksheet_name, rows=1000, cols=max(1, len(df.columns)))
        logging.info(f"Worksheet '{worksheet_name}' created.")

    # Clear & resize
    ws.clear()
    rows = len(df) + 1  # header
    cols = len(df.columns)
    ws.resize(rows=rows, cols=cols)

    # Prepare values (header + rows)
    values = [df.columns.tolist()] + df.astype(str).where(pd.notna(df), "").values.tolist()

    # Write in chunks to avoid payload limits
    CHUNK = 5000  # rows per request
    for start in range(0, len(values), CHUNK):
        chunk = values[start : start + CHUNK]
        range_start_row = start + 1
        range_end_row = start + len(chunk)
        cell_range = f"A{range_start_row}:"
        retry_api_call(lambda: ws.update(cell_range, chunk, value_input_option="RAW"))

    logging.info(f"Updated '{worksheet_name}' with {len(df)} rows.")


def update_google_sheet(df: pd.DataFrame, sheet_id: str):
    """Authorize and update the Google Sheet."""
    logging.info("Loading Google credentials...")

    creds_env = os.getenv("GGL_CREDENTIALS")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if creds_env:
        creds = Credentials.from_service_account_info(json.loads(creds_env), scopes=scope)
    else:
        creds = Credentials.from_service_account_file("notas-transf.json", scopes=scope)

    client = gspread.authorize(creds)

    update_worksheet(df, sheet_id, "transf_total", client)


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    sheet_id = os.getenv("sheet_id")
    if not sheet_id:
        logging.error("Environment variable 'sheet_id' not set.")
        return

    download_dir = "/home/runner/work/notas_transf/notas_transf"  # adjust as needed

    all_files = get_all_files(directory=download_dir, extensions=("xls", "xlsx"))
    if not all_files:
        logging.warning("No Excel files found in the directory.")
        return

    logging.info(f"Found {len(all_files)} file(s) to process.")

    dfs = []
    for f in all_files:
        try:
            cleaned = clean_transfer_file(f)
            logging.info(f"  -> {os.path.basename(f)}: {len(cleaned)} clean rows")
            if len(cleaned) > 0:
                dfs.append(cleaned)
        except Exception as e:
            logging.exception(f"Failed processing {os.path.basename(f)}: {e}")

    if not dfs:
        logging.warning("No dataframes produced after cleaning. Nothing to upload.")
        return

    final_df = pd.concat(dfs, ignore_index=True)
    logging.info(f"Final combined rows: {len(final_df)}")

    update_google_sheet(final_df, sheet_id)


if __name__ == "__main__":
    main()
