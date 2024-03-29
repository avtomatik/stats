#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  3 11:09:07 2023

@author: green-machine
"""


from pathlib import Path

import yaml
from constants import (DATA_CAPITAL_ACQUISITIONS, DATA_CAPITAL_RETIREMENT,
                       TITLES_DOUGLAS)

from stats.src.can.constants import (DATA_CONSTRUCT_CAN_FORMER,
                                     DATA_CONSTRUCT_CAN_FORMER_NOT_USED,
                                     DATA_STATCAN_ARCHIVE)
from thesis.src.lib.constants import READ_USA_HIST


def dump(path_exp: str, file_name: str, data: dict) -> None:
    with open(Path(path_exp).joinpath(file_name), 'w') as f:
        yaml.dump(data, f, default_flow_style=False)


def can(path_exp: str = '/home/green-machine/Downloads') -> None:

    file_name = 'combine_can_former.yaml'
    dump(path_exp, file_name, DATA_CONSTRUCT_CAN_FORMER)

    file_name = 'combine_can_former_not_used.yaml'
    dump(path_exp, file_name, DATA_CONSTRUCT_CAN_FORMER_NOT_USED)

    file_name = 'statcan_archive.yaml'
    dump(path_exp, file_name, DATA_STATCAN_ARCHIVE)


def usa(path_exp: str = '/home/green-machine/Downloads') -> None:
    # =========================================================================
    # dataset_usa_cobb_douglas_markup.py
    # =========================================================================

    file_name = 'capital_acquisitions.yaml'
    dump(path_exp, file_name, DATA_CAPITAL_ACQUISITIONS)

    file_name = 'capital_retirement.yaml'
    dump(path_exp, file_name, DATA_CAPITAL_RETIREMENT)

    file_name = 'read_usa_hist.yaml'
    dump(path_exp, file_name, READ_USA_HIST)

    file_name = 'plot_douglas.yaml'
    dump(path_exp, file_name, TITLES_DOUGLAS)


if __name__ == '__main__':
    usa()
    can()
