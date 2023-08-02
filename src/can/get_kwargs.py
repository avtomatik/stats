#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug  2 21:41:31 2023

@author: green-machine
"""

# =============================================================================
# Not Clear
# =============================================================================


def get_kwargs():
    archive_id = 310003
    file_id = 7591839622055840674

    return {
        'filepath_or_buffer': f'dataset_read_can-{archive_id:08n}-eng-{file_id}.csv',
        'skiprows': 3,
    }
