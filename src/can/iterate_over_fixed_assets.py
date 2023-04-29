#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 12:45:19 2023

@author: green-machine
"""


from thesis.src.lib.read import read_can

# =============================================================================
# Capital
# =============================================================================
archive_id = 36100096
df = read_can(archive_id)
for col_num, _ in enumerate(df.columns):
    values = sorted(set(df.iloc[:, col_num]))
    print(_)
