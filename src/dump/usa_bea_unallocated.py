#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 14:10:29 2023

@author: green-machine
"""

from pathlib import Path

import pandas as pd
from pandas import DataFrame


def retrieve(df: DataFrame) -> DataFrame:
    df = df[~df.iloc[:, 0].str.contains('Depreciation')]
    df = df[df.iloc[:, 1].str.contains('Billions')]
    df = df[~df.iloc[:, 1].str.contains('Years')]
    return df


path_src = '/media/green-machine/KINGSTON'
file_name = 'dataset_usa_bea-nipa-2017-08-23-sfat.zip'
# =============================================================================
# Retrieve Series' Codes
# =============================================================================
df = pd.read_csv(Path(path_src).joinpath(file_name)).pipe(retrieve)


file_name = 'dataset_usa_bea-sfat-release-2017-08-23-Vectors.csv'
# =============================================================================
# Retrieve Tables' Titles
# =============================================================================
df = pd.read_csv(Path(path_src).joinpath(file_name)).pipe(retrieve)
