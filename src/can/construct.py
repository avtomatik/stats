# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 22:20:54 2021

@author: Alexander Mikhailov
"""


import pandas as pd
from can.constants import (BLUEPRINT_CAPITAL, BLUEPRINT_LABOUR,
                           BLUEPRINT_PRODUCT)
from can.stockpile import stockpile_can
from common.funcs import get_pre_kwargs
from core.config import BASE_DIR


def main(
    series_ids: dict[str, int],
    file_name: str = 'stat_can_desc.csv'
):
    FILE_NAMES = ('stat_can_cap.csv', 'stat_can_lab.csv', 'stat_can_prd.csv')
    # =========================================================================
    # Construct CSV File from Specification
    # =========================================================================
    stockpile_can(series_ids)

    # =========================================================================
    # Retrieve Series Description
    # =========================================================================
    df = pd.concat(
        map(lambda _: pd.read_csv(**get_pre_kwargs(_)), FILE_NAMES),
        axis=1
    )

    pd.merge(
        pd.read_csv(**get_pre_kwargs(file_name)),
        df.transpose(),
        left_index=True,
        right_index=True,
    ).transpose().to_csv(BASE_DIR.joinpath(file_name))


if __name__ == '__main__':
    main(BLUEPRINT_CAPITAL | BLUEPRINT_LABOUR | BLUEPRINT_PRODUCT)
