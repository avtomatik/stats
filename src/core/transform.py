#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug  6 20:58:48 2023

@author: green-machine
"""

import pandas as pd


def transform_year_mean(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(df.index.year).mean()
