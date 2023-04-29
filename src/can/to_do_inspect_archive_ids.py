#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 23:09:17 2023

@author: green-machine
"""

from thesis.src.lib.read import read_can

ARCHIVE_IDS = [3800106, 3800566, 18100081, 36100210]
for archive_id in ARCHIVE_IDS:
    print(read_can(archive_id))
