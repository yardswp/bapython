import os

import pandas as pd
from dotenv import find_dotenv, load_dotenv


load_dotenv(find_dotenv())


files_dir = os.getenv('BA_FILES_DIR', '')


def loadFromExcel(name: str, sheet: str = None) -> pd.DataFrame:
    if sheet is None:
        sheet = name

    xlsx_file = os.path.join(files_dir, name + '.xlsx')
    if os.path.exists(xlsx_file):
        return pd.read_excel(xlsx_file, sheet)

    xlsm_file = os.path.join(files_dir, name + '.xlsm')
    if os.path.exists(xlsm_file):
        return pd.read_excel(xlsm_file, sheet)

    raise FileNotFoundError(f'File {xlsx_file} or {xlsm_file}')
