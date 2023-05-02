# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import io
import os
from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
from pandas import DataFrame

from ..common.transform import transform_year_mean
from ..common.utils import dichotomize_series_ids
from .combine import combine_can_plain_or_sum
from .constants import (MAP_READ_CAN_SPC, SERIES_IDS_INDEXES,
                        SERIES_IDS_PERSONS, SERIES_IDS_THOUSANDS)
from .get_mean_for_min_std import get_mean_for_min_std


@cache
def read_can(archive_id: int) -> DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = {'period': 0, 'series_id': 9, 'value': 11}
    TO_PARSE_DATES = (2820011, 3790031, 3800084, 14100221,
                      14100235, 14100238, 14100355, 36100108, 36100207, 36100434)
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'

    kwargs = {
        'header': 0,
        'names': list(MAP_READ_CAN_SPC.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_READ_CAN_SPC.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }

    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'dataset_can_{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = ZipFile(
                io.BytesIO(requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


def combine_can_special(
    series_ids_plain: dict[str, int],
    series_ids_mean: dict[str, int]
) -> DataFrame:
    if series_ids_plain:
        return combine_can_plain_or_sum(series_ids_plain)
    if series_ids_mean:
        return combine_can_plain_or_sum(series_ids_mean).pipe(transform_year_mean)


def get_data_frame_index(series_ids: tuple[dict[str, int]]) -> DataFrame:
    df = pd.concat(
        map(
            lambda _: combine_can_special(
                *dichotomize_series_ids(_, TO_PARSE_DATES)
            ).pct_change(),
            series_ids
        ),
        axis=1,
        sort=True
    )
    df['mean'] = df.mean(axis=1)
    # =========================================================================
    # Composite Index
    # =========================================================================
    df['composite'] = df.iloc[:, [-1]].add(1).cumprod()
    # =========================================================================
    # Patch First Element
    # =========================================================================
    df['composite'] = df['composite'].fillna(1)
    return df.iloc[:, [-1]]


def get_data_frame_value(
    series_ids_thousands: tuple[dict[str, int]],
    series_ids_persons: tuple[dict[str, int]]
) -> DataFrame:
    df = pd.concat(
        [
            pd.concat(
                map(
                    lambda _: combine_can_special(
                        *dichotomize_series_ids(_, TO_PARSE_DATES)
                    ),
                    series_ids_thousands
                ),
                axis=1
            ),
            pd.concat(
                map(
                    lambda _: combine_can_special(
                        *dichotomize_series_ids(_, TO_PARSE_DATES)
                    ).div(1000),
                    series_ids_persons
                ),
                axis=1
            )
        ],
        axis=1,
        sort=True
    )
    df['mean'] = df.mean(axis=1)
    return df.iloc[:, [-1]]


PATH_SOURCE = '../../../data/external'
PATH_EXPORT = '/home/green-machine/Downloads'

TO_PARSE_DATES = (2820011, 3790031, 3800084, 14100221, 14100235,
                  14100238, 14100355, 36100108, 36100207, 36100434)

# =============================================================================
# Labor
# =============================================================================
SERIES_IDS = {
    '!v41707595': 36100309,  # Not Useful: Labour input
    '!v41712954': 36100208,  # Not Useful: Labour input
    '!v42189231': 36100310,  # Not Useful: Labour input
    '!v65522120': 36100489,  # Not Useful
    '!v65522415': 36100489,  # Not Useful
}


os.chdir(PATH_SOURCE)


def main(path_export, SERIES_IDS_INDEXES, SERIES_IDS_THOUSANDS, SERIES_IDS_PERSONS):
    df_index = get_data_frame_index(SERIES_IDS_INDEXES)

    # =============================================================================
    # df_value = get_data_frame_value(SERIES_IDS_THOUSANDS, SERIES_IDS_PERSONS)
    # =============================================================================

    df = DataFrame()

    year, value = get_mean_for_min_std(SERIES_IDS_THOUSANDS)

    df['workers'] = df_index.div(df_index.loc[year, :]).mul(value)
    df = df.iloc[:, [-1]].round(1)

    file_name = 'can_labour.pdf'
    df.plot(grid=True).get_figure().savefig(
        Path(path_export).joinpath(file_name),
        format='pdf',
        dpi=900
    )


if __name__ == '__main__':
    main(
        PATH_EXPORT,
        SERIES_IDS_INDEXES,
        SERIES_IDS_THOUSANDS,
        SERIES_IDS_PERSONS
    )
