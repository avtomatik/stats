# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 22:20:54 2021

@author: Alexander Mikhailov
"""


from itertools import combinations
from pathlib import Path

import pandas as pd
from pandas import DataFrame
from sklearn.metrics import r2_score
from thesis.src.lib.read import read_temporary

# =============================================================================
# TODO: What?
# =============================================================================
df = pd.read_csv(filepath_or_buffer, skiprows=range(1, 7), index_col=0)

matches = []
for pair in combinations(df.columns, 2):
    chunk = df.loc[:, list(pair)].dropna(axis=0)
    if (not chunk.empty) & chunk.iloc[:, 0].equals(chunk.iloc[:, 1]):
        matches.append(pair)
for pair in matches:
    print(pair)

FILE_NAME = 'stat_can_cap.csv'
data = read_temporary(FILE_NAME)


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
df.to_csv(Path(DIR_EXPORT).joinpath('df.csv'), index=False)
