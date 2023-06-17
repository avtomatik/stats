#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 22 12:28:16 2022

@author: Alexander Mikhailov
"""


import xml.etree.ElementTree as et
from pathlib import Path, PosixPath
from typing import Union
from zipfile import ZipFile

import pandas as pd
from pandas import DataFrame


def read_usa_frb_archive(filepath: Union[str, PosixPath]) -> DataFrame:
    kwargs = {
        'index_col': 0,
        'skiprows': 4
    }
    with ZipFile(filepath) as archive:
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
PATH_SRC = '../data/external'
FILE_NAME = 'frb_g17_2.csv'
FILE_NAME = 'frb_g17_3.csv'
FILE_NAME = 'FRB_H15.zip'

SERIES_ID = 'CAPUTL.B00004.S'
filepath = Path(PATH_SRC).joinpath(FILE_NAME)
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


PATH_SRC = '../data/external'
FILE_NAME = 'FRB_H15.zip'

filepath = Path(PATH_SRC).joinpath(FILE_NAME)
read_usa_frb_archive(filepath)
