import pandas as pd
import os
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

folder = "/home/runner/work/notas_transf/notas_transf/downloads"  # change if needed

for file in os.listdir(folder):
    file_path = os.path.join(folder, file)

    # Process only .xls and .xlsx files
    if file.lower().endswith((".xls", ".xlsx")):
        logging.info(f"Processing: {file}")

        try:
            # Read file (skip first 12 rows)
            df = pd.read_excel(file_path, skiprows=11)

            # Convert .xls → .xlsx
            new_path = str(Path(file_path).with_suffix(".xlsx"))

            # Save converted file
            df.to_excel(new_path, index=False)
            logging.info(f"Saved: {new_path}")

            # Delete ONLY if original was .xls
            if file.lower().endswith(".xls"):
                os.remove(file_path)
                logging.info(f"Deleted original: {file_path}")

        except Exception as e:
            logging.info(f"Error processing {file}: {e}")
