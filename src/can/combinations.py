# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 22:20:54 2021

@author: Alexander Mikhailov
"""


from itertools import combinations
from pathlib import Path

import pandas as pd
from common.funcs import get_pre_kwargs
from pandas import DataFrame
from sklearn.metrics import r2_score

PATH_EXPORT = '/home/green-machine/Downloads'

FILE_NAME = 'stat_can_cap.csv'
data = pd.read_csv(**get_pre_kwargs(FILE_NAME))


df = DataFrame(columns=['series_id_1', 'series_id_2', 'r_2'])
for pair in combinations(data.columns, 2):
    chunk = data.loc[:, list(pair)].dropna(axis=0)
    if not chunk.empty:
        df = df.append(
            {
                'series_id_1': pair[0],
                'series_id_2': pair[1],
                'r_2': r2_score(chunk.iloc[:, 0], chunk.iloc[:, 1])
            },
            ignore_index=True
        )
FILE_NAME = 'data_composed.csv'
kwargs = {
    'path_or_buf': Path(PATH_EXPORT).joinpath(FILE_NAME),
    'index': False
}
df.to_csv(**kwargs)
