#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 11:09:07 2023

@author: green-machine
"""


import yaml
from constants import (DATA_CAPITAL_ACQUISITIONS, DATA_CAPITAL_RETIREMENT,
                       TITLES_DOUGLAS)

from stats.src.can.constants import (DATA_CONSTRUCT_CAN_FORMER,
                                     DATA_CONSTRUCT_CAN_FORMER_NOT_USED,
                                     DATA_STATCAN_ARCHIVE)
from thesis.src.lib.constants import READ_USA_HIST


def dump(file_path, data: dict) -> None:
    with file_path.open('w') as f:
        yaml.dump(data, f, default_flow_style=False)


def can() -> None:

    file_path = 'combine_can_former.yaml'
    dump(file_path, DATA_CONSTRUCT_CAN_FORMER)

    file_path = 'combine_can_former_not_used.yaml'
    dump(file_path, DATA_CONSTRUCT_CAN_FORMER_NOT_USED)

    file_path = 'statcan_archive.yaml'
    dump(file_path, DATA_STATCAN_ARCHIVE)


def usa() -> None:
    # =========================================================================
    # dataset_usa_cobb_douglas_markup.py
    # =========================================================================

    file_path = 'capital_acquisitions.yaml'
    dump(file_path, DATA_CAPITAL_ACQUISITIONS)

    file_path = 'capital_retirement.yaml'
    dump(file_path, DATA_CAPITAL_RETIREMENT)

    file_path = 'read_usa_hist.yaml'
    dump(file_path, READ_USA_HIST)

    file_path = 'plot_douglas.yaml'
    dump(file_path, TITLES_DOUGLAS)


if __name__ == '__main__':
    usa()
    can()
