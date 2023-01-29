#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 22 12:28:16 2022

@author: Alexander Mikhailov
"""


import pandas as pd

filepath_or_buffer = 'FRB_G17(2).csv'
filepath_or_buffer = 'FRB_G17(3).csv'
SERIES_ID = 'CAPUTL.B00004.S'
kwargs = {'filepath_or_buffer': filepath_or_buffer}
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
# def read_usa_frb() -> DataFrame:
#    with ZipFile(Path(DIR).joinpath(FILE_NAME)) as archive:
#        _map = {_.file_size: _.filename for _ in archive.filelist}
#        print(_map[max(_map.keys())])
#        # =====================================================================
#        # Select the Largest File
#        # =====================================================================
#        with archive.open(_map[max(_map.keys())]) as f:
#            df = pd.read_csv(f, skiprows=4)
#            df.dropna(axis=1, how='all', inplace=True)
#            df.set_index(df.columns[0], inplace=True)
#            df = df.transpose()
#            df = df.drop(df.index[:3])
#            return df.rename_axis('period')
#            xtree = et.parse(_map[max(_map.keys())])
#            xroot = xtree.getroot()
