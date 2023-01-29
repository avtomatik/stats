#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 12:12:29 2023

@author: green-machine
"""

import pandas as pd

kwargs = {
    'sep': '\t',
    'index_col': range(4),
    'usecols': 1,
}
kwargs['filepath_or_buffer'] = '/home/green-machine/data_science/macroeconomics/usa_science_data.zip/pc.df.0.Current'
df = pd.read_csv(**kwargs)
kwargs['filepath_or_buffer'] = '/home/green-machine/data_science/macroeconomics/usa_science_data.zip/ap.df.0.Current'
df = pd.read_csv(**kwargs)
