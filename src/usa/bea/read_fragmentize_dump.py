#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 12:48:53 2023

@author: green-machine
"""

from pathlib import Path

import pandas as pd
from pandas import DataFrame

# =============================================================================
# Bureau of Economic Analysis
# =============================================================================


def read(file_name: str, path_src: str) -> DataFrame:
    MAP = {
        'source_id': 0, 'series_id': 14, 'period': 15, 'subperiod': 16, 'value': 17
    }
    kwargs = {
        'filepath_or_buffer': Path(path_src).joinpath(file_name),
        'header': 0,
        'names': tuple(MAP.keys()),
        'index_col': 2,
        'usecols': tuple(MAP.values()),
    }
    return pd.read_csv(**kwargs)


def extract_yearly_data(df: DataFrame) -> DataFrame:
    # =========================================================================
    # Yearly Data
    # =========================================================================
    df = df[df.loc[:, "subperiod"] == 0]
    return df.drop("subperiod", axis=1)


def fragmentize_dump(df: DataFrame, series_ids: tuple[str], path_exp: str) -> None:
    for series_id in series_ids:
        df.pipe(read_usa_bea_pull_by_series_id, series_id=series_id).to_csv(
            Path(path_exp).joinpath(
                f'dataset_usa_bea-nipa-2015-05-01-{series_id}.csv'
            )
        )


def main(
    file_name: str = 'dataset_usa_bea-nipa-2015-05-01.zip',
    path_src: str = '/media/green-machine/KINGSTON'
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
    # =========================================================================
    # read(file_name, path_src).pipe(extract_yearly_data).pipe(
    #     fragmentize_dump(SERIES_IDS, path_src)
    # )
    # =========================================================================


if __name__ == '__main__':
    main()
