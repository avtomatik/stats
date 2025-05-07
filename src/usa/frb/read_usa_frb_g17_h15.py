#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 22 12:28:16 2022

@author: Alexander Mikhailov
"""


import xml.etree.ElementTree as et
import zipfile
from pathlib import Path
from typing import Union

import pandas as pd
from core.config import DATA_DIR


def read_usa_frb_archive(filepath: Union[str, Path]) -> pd.DataFrame:
    kwargs = {
        'index_col': 0,
        'skiprows': 4
    }
    with zipfile.ZipFile(filepath) as archive:
        # =====================================================================
        # Select the Largest File with min() Function
        # =====================================================================
        with archive.open(
            min({_.filename: _.file_size for _ in archive.filelist})
        ) as f:
            kwargs['filepath_or_buffer'] = f
            df = pd.read_csv(**kwargs).dropna(axis=1, how='all').transpose()
            return df.drop(df.index[:3]).rename_axis('period')
            # =================================================================
            # TODO: Further Development
            # =================================================================
            xtree = et.parse(min(MAP_FILES))
            xroot = xtree.getroot()


FILE_NAME = 'FRB_G17(2).csv'
FILE_NAME = 'FRB_G17(3).csv'
FILE_NAME = 'frb_g17_2.csv'
FILE_NAME = 'frb_g17_3.csv'
FILE_NAME = 'FRB_H15.zip'

SERIES_ID = 'CAPUTL.B00004.S'
filepath = DATA_DIR.joinpath(FILE_NAME)
kwargs = {'filepath_or_buffer': filepath}
# =====================================================================
# Load
# =====================================================================
df = pd.read_csv(**kwargs)
kwargs['index_col'] = 0
kwargs['usecols'] = range(5, df.shape[1])
# =====================================================================
# Re-Load
# =====================================================================
df = pd.read_csv(**kwargs).transpose().rename_axis('period')
df.index = pd.to_datetime(df.index)


SERIES_IDS = (
    'CAPUTL.B00004.S',  # Use This
    'CAPUTL.GMF.S',
)
df_a = df.loc[:, SERIES_IDS].groupby(df.index.year).mean()
df.groupby(df.index.year).mean().plot(grid=True)



FILE_NAME = 'FRB_H15.zip'


filepath = DATA_DIR.joinpath(FILE_NAME)
read_usa_frb_archive(filepath)
