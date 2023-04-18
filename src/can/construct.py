# -*- coding: utf-8 -*-
"""
Created on Sat Sep 18 22:20:54 2021

@author: Alexander Mikhailov
"""


from pathlib import Path

import pandas as pd

from stats.src.can.constants import (BLUEPRINT_CAPITAL, BLUEPRINT_LABOUR,
                                     BLUEPRINT_PRODUCT)
from stats.src.can.labour import stockpile_can


def main(
    series_ids: dict[str, int],
    path_src: str = '../data/external',
    file_name: str = 'stat_can_desc.csv'
):
    # =========================================================================
    # Construct CSV File from Specification
    # =========================================================================
    stockpile_can(series_ids)

    # =========================================================================
    # Retrieve Series Description
    # =========================================================================
    df = pd.concat(
        map(lambda _: read_temporary(_), FILE_NAMES),
        axis=1
    )

    desc = pd.merge(
        read_temporary(file_name),
        df.transpose(),
        left_index=True,
        right_index=True,
    )
    desc.transpose().to_csv(Path(path_src).joinpath(file_name))


if __name__ == '__main__':
    main(BLUEPRINT_CAPITAL | BLUEPRINT_LABOUR | BLUEPRINT_PRODUCT)
