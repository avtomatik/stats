# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import io
import os
import zipfile
from functools import cache
from pathlib import Path

import pandas as pd
import requests
from core.config import BASE_DIR, DATA_DIR

from ..common.funcs import dichotomize_series_ids
from ..common.transform import transform_year_mean
from .combine import combine_can_plain_or_sum
from .constants import (MAP_READ_CAN_SPC, SERIES_IDS_INDEXES,
                        SERIES_IDS_PERSONS, SERIES_IDS_THOUSANDS)
from .get_mean_for_min_std import get_mean_for_min_std


@cache
def read_can(archive_id: int) -> pd.DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 9, 11]))
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                io.BytesIO(requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


def combine_can_special(
    series_ids_plain: dict[str, int],
    series_ids_mean: dict[str, int]
) -> pd.DataFrame:
    if series_ids_plain:
        return combine_can_plain_or_sum(series_ids_plain)
    if series_ids_mean:
        return combine_can_plain_or_sum(series_ids_mean).pipe(transform_year_mean)


def get_data_frame_index(series_ids: tuple[dict[str, int]]) -> pd.DataFrame:
    df = pd.concat(
        map(
            lambda _: combine_can_special(
                *dichotomize_series_ids(_, TO_PARSE_DATES)
            ),
            series_ids
        ),
        axis=1,
        sort=True
    ).pct_change()
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
) -> pd.DataFrame:
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


TO_PARSE_DATES = (
    2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
)

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


os.chdir(DATA_DIR)


def main(SERIES_IDS_INDEXES, SERIES_IDS_THOUSANDS, SERIES_IDS_PERSONS):
    df_index = get_data_frame_index(SERIES_IDS_INDEXES)

    # =============================================================================
    # df_value = get_data_frame_value(SERIES_IDS_THOUSANDS, SERIES_IDS_PERSONS)
    # =============================================================================

    df = pd.DataFrame()

    year, value = get_mean_for_min_std(SERIES_IDS_THOUSANDS, TO_PARSE_DATES)

    df['workers'] = df_index.div(df_index.loc[year, :]).mul(value).round(1)

    FILE_NAME = 'can_labour.pdf'
    kwargs = {
        'fname': BASE_DIR.joinpath(FILE_NAME),
        'format': 'pdf',
        'dpi': 900
    }
    df.plot(grid=True).get_figure().savefig(**kwargs)


if __name__ == '__main__':
    main(
        SERIES_IDS_INDEXES,
        SERIES_IDS_THOUSANDS,
        SERIES_IDS_PERSONS
    )
