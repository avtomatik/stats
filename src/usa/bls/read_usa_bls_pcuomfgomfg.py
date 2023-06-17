#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 29 12:15:52 2023

@author: green-machine
"""


from thesis.src.lib.read import read_usa_fred

SERIES_IDS = ['PCUOMFGOMFG', 'PPIACO', 'PRIME']

for series_id in SERIES_IDS:
    print(read_usa_fred(series_id))
