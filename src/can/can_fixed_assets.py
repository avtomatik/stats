#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 12:45:19 2023

@author: green-machine
"""

df = construct_can()
archive_id = 36100096
df_cap = read_can(archive_id)
# =============================================================================
# df_cap = read_can_capital(fetch_can_capital_query())
# =============================================================================
for col_num, _ in enumerate(df_cap.columns):
    values = sorted(set(df_cap.iloc[:, col_num]))
    print(_)
