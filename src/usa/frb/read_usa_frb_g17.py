from pathlib import Path
from zipfile import ZipFile

import pandas as pd

pd.options.display.max_columns = 8


DIR = '/home/green-machine/data_science/data/external'
FILE_NAME = 'FRB_g17.zip'
with ZipFile(Path(DIR).joinpath(FILE_NAME)) as archive:
    MAP_FILES = {_.file_size: _.filename for _ in archive.filelist}
    # =====================================================================
    # Select the Largest File
    # =====================================================================
    with archive.open(MAP_FILES[max(MAP_FILES)]) as f:
        df = pd.read_xml(
            f,
            xpath=".//frb:DataSet",
            namespaces={
                "kf": "http://www.federalreserve.gov/structure/compact/G17_IP_MAJOR_INDUSTRY_GROUPS"}
        )
        df.to_excel('test.xlsx', index=False)
        df = pd.read_xml(
            f,
            index_col=0,
            skiprows=4
        ).dropna(axis=1, how='all').transpose()
        df.drop(df.index[:3], inplace=True)
        # return df.rename_axis('period')
