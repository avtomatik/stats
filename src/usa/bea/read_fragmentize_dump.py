#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 12:48:53 2023

@author: green-machine
"""


import pandas as pd
from core.config import BASE_DIR, DATA_DIR

from stats.src.usa.bea.read import get_kwargs, read_usa_bea_pull_by_series_id


def extract_yearly_data(df: pd.DataFrame, column: str = "subperiod") -> pd.DataFrame:
    # =========================================================================
    # Yearly Data
    # =========================================================================
    return df[df.loc[:, column] == 0].drop(column, axis=1)


def fragmentize_dump(df: pd.DataFrame, series_ids: tuple[str]) -> None:
    for series_id in series_ids:
        kwargs = {
            'path_or_buf': BASE_DIR.joinpath(f'dataset_usa_bea-nipa-2015-05-01-{series_id}.csv')
        }
        df.pipe(
            read_usa_bea_pull_by_series_id, series_id=series_id
        ).to_csv(**kwargs)


def main(
    file_name: str = 'dataset_usa_bea-nipa-2015-05-01.zip',
) -> None:

    SERIES_IDS = (
        # =====================================================================
        # Not Over There 'K100701'
        # =====================================================================
        'A006RC1', 'A019RC1', 'A027RC1', 'A030RC1', 'A032RC1', 'A051RC1',
        'A052RC1', 'A054RC1', 'A061RC1', 'A065RC1', 'A067RC1', 'A124RC1',
        'A191RC1', 'A191RX1', 'A229RC0', 'A229RX0', 'A262RC1', 'A390RC1',
        'A392RC1', 'A399RC1', 'A400RC1', 'A4601C0', 'A655RC1', 'A822RC1',
        'A929RC1', 'B057RC0', 'B230RC0', 'B394RC1', 'B645RC1', 'DPCERC1',
        'W055RC1', 'W056RC1'
    )

    pd.read_csv(**get_kwargs(DATA_DIR.joinpath(file_name))).pipe(
        extract_yearly_data
    ).pipe(
        fragmentize_dump, SERIES_IDS
    )


if __name__ == '__main__':
    main()
