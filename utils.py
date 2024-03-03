import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv())


files_dir = os.getenv('BA_FILES_DIR', '')

type MaybeDataFrame = False | pd.DataFrame


def maybe_load_xl_dataframe(name: str, sheet: str) -> MaybeDataFrame:
    xl_file = os.path.join(files_dir, name)
    if os.path.exists(xl_file):
        print(f'Loading data from {name}')
        return pd.read_excel(xl_file, sheet)
    return False


def loadFromExcel(name: str, sheet: str = None) -> pd.DataFrame:
    if sheet is None:
        sheet = name

    df_or_false = maybe_load_xl_dataframe(name + '.xlsx', sheet)
    if not isinstance(df_or_false, pd.DataFrame):
        df_or_false = maybe_load_xl_dataframe(name + '.xlsm', sheet)
        if not isinstance(df_or_false, pd.DataFrame):
            raise FileNotFoundError(f'File {name}.xlsx or {name}.xlsm not found')

    return df_or_false

