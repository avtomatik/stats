#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 12:12:29 2023

@author: green-machine
"""

from pathlib import Path

import pandas as pd

DIR = '../macroeconomics'
kwargs = {
    'sep': '\t',
    'index_col': range(4),
    'usecols': 1,
}
FILE_NAME = 'usa_science_data.zip/ap.df.0.Current'
FILE_NAME = 'usa_science_data.zip/pc.df.0.Current'

kwargs['filepath_or_buffer'] = Path(DIR).joinpath(FILE_NAME)
df = pd.read_csv(**kwargs)
