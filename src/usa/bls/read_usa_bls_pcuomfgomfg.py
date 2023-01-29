#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 12:15:52 2023

@author: green-machine
"""


import io

import pandas as pd
import requests
from pandas import DataFrame


def read_usa_fred(series_id: str) -> DataFrame:
    """
    ('PCUOMFGOMFG')

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series
        ================== =================================
    """
    url = f'https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}'
    kwargs = {
        'filepath_or_buffer': io.BytesIO(requests.get(url).content),
        'header': 0,
        'names': ('period', series_id.lower()),
        'index_col': 0,
        'parse_dates': True
    }
    df = pd.read_csv(**kwargs)
    return df.groupby(df.index.year).mean()


print(read_usa_fred('PCUOMFGOMFG'))
