# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 22:20:54 2021

@author: Alexander Mikhailov
"""


from itertools import combinations

import pandas as pd

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
