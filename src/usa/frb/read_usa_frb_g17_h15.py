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


def read_usa_frb(filepath: Union[str, PosixPath]) -> DataFrame:
    with ZipFile(filepath) as archive:
        MAP_FILES = {_.filename: _.file_size for _ in archive.filelist}
        # =====================================================================
        # Select the Largest File
        # =====================================================================
        with archive.open(max(MAP_FILES, key=MAP_FILES.get)) as f:
            # =================================================================
            # TODO: Further Development
            # =================================================================
            df = pd.read_csv(f, skiprows=4)
            df.dropna(axis=1, how='all', inplace=True)
            df.set_index(df.columns[0], inplace=True)
            df = df.transpose()
            df = df.drop(df.index[:3])
            return df.rename_axis('period')
            xtree = et.parse(MAP_FILES[max(MAP_FILES.keys())])
            xroot = xtree.getroot()


DIR = '../data/external'
FILE_NAME = 'frb_g17_2.csv'
FILE_NAME = 'frb_g17_3.csv'
# FILE_NAME = 'FRB_H15.zip'

SERIES_ID = 'CAPUTL.B00004.S'
kwargs = {'filepath_or_buffer': Path(DIR).joinpath(FILE_NAME)}
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

read_usa_frb(Path(DIR).joinpath(FILE_NAME))
