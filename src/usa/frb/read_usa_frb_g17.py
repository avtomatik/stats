from pathlib import Path
from zipfile import ZipFile

import pandas as pd

pd.options.display.max_columns = 8


PATH = '../data/external'
FILE_NAME = 'FRB_g17.zip'
with ZipFile(Path(PATH).joinpath(FILE_NAME)) as archive:
    MAP_FILES = {_.filename: _.file_size for _ in archive.filelist}
    # =====================================================================
    # Select the Largest File with min() Function
    # =====================================================================
    with archive.open(min(MAP_FILES)) as f:
        kwargs = {
            'path_or_buffer': f,
            'xpath': ".//frb:DataSet",
            'namespaces': {
                "kf": "http://www.federalreserve.gov/structure/compact/G17_IP_MAJOR_INDUSTRY_GROUPS"
            }
        }
        df = pd.read_xml(**kwargs)
        kwargs = {
            'path_or_buffer': f,
            'index_col': 0,
            'skiprows': 4
        }
        df = pd.read_xml(**kwargs).dropna(axis=1, how='all').transpose()
        df.drop(df.index[:3], inplace=True)
        # return df.rename_axis('period')
