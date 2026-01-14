import io
import zipfile
import pandas as pd
import os


class CsvHandler:
    def __init__(self, logger):
        self.logger = logger

    def extract_dataframe(self, zip_bytes: bytes) -> pd.DataFrame:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            csv_files = [f for f in z.namelist() if f.endswith(".csv")]
            if not csv_files:
                raise ValueError("Geen CSV gevonden in ZIP")

            csv_name = csv_files[0]
            self.logger.info(f"CSV gevonden: {csv_name}")

            z.extract(csv_name, "data")
            return pd.read_csv(os.path.join("data", csv_name))
