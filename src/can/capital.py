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
from core.config import DATA_DIR

from stats.src.can.constants import BLUEPRINT_CAPITAL, MAP_READ_CAN_SPC

# =============================================================================
# from stats.src.can.constants import BLUEPRINT_CAPITAL
# =============================================================================

# =============================================================================
# TODO: Clear It Up
# =============================================================================


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


def pull_by_series_id(df: pd.DataFrame, series_id: str) -> pd.DataFrame:
    """


    Parameters
    ----------
    df : pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series IDs
        df.iloc[:, 1]      Values
        ================== =================================
    series_id : str

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series
        ================== =================================
    """
    assert df.shape[1] == 2
    return df[df.iloc[:, 0] == series_id].iloc[:, [1]].rename(
        columns={'value': series_id}
    )


def stockpile_can(series_ids: dict[str, int]) -> pd.DataFrame:
    """
    Parameters
    ----------
    series_ids : dict[str, int]
        DESCRIPTION.
    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    return pd.concat(
        map(
            lambda _: read_can(_[-1]).pipe(pull_by_series_id, _[0]),
            series_ids.items()
        ),
        axis=1,
        sort=True
    )


def transform_year_mean(df: pd.DataFrame) -> pd.DataFrame:
    return df.groupby(df.index.year).mean()


def transform_sum(df: pd.DataFrame, name: str) -> pd.DataFrame:
    """


    Parameters
    ----------
    df : pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, ...]    Series
        ================== =================================
    name : str
        New Column Name.

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Sum of <series_ids>
        ================== =================================
    """
    df[name] = df.sum(axis=1)
    return df.iloc[:, [-1]]


def combine_can_plain_or_sum(series_ids: dict[str, int]) -> pd.DataFrame:
    if len(series_ids) > 1:
        return stockpile_can(series_ids).pipe(
            transform_sum,
            name='_'.join((*series_ids, 'sum'))
        )
    return stockpile_can(series_ids)


def combine_can_special(
    series_ids_plain: dict[str, int],
    series_ids_mean: dict[str, int]
) -> pd.DataFrame:
    if series_ids_plain:
        return combine_can_plain_or_sum(series_ids_plain)
    if series_ids_mean:
        return combine_can_plain_or_sum(series_ids_mean).pipe(transform_year_mean)


def dichotomize_series_ids(
    series_ids: dict[str, int],
    source_ids: tuple[int]
) -> tuple[dict[str, int]]:
    """
    Parameters
    ----------
    series_ids : dict[str, int]
        DESCRIPTION.
    source_ids : tuple[int]
        DESCRIPTION.
    Returns
    -------
    tuple[dict[str, int]]
        DESCRIPTION.
    """
    return (
        {
            key: value for key, value in series_ids.items() if not value in source_ids
        },
        {
            key: value for key, value in series_ids.items() if value in source_ids
        }
    )


TO_PARSE_DATES = (
    2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
)

os.chdir(DATA_DIR)

# =============================================================================
# Product
# =============================================================================

SERIES_IDS = (
    {'v37482': 10100094},  # Not Useful: Capacity Utilization
    {'v4331088': 16100109},  # Not Useful: Capacity Utilization
    {'v142817': 16100111},  # Not Useful: Capacity Utilization
)

df = pd.concat(
    map(
        lambda _: combine_can_special(
            *dichotomize_series_ids(_, TO_PARSE_DATES)
        ),
        SERIES_IDS
    ),
    axis=1,
    sort=True
)

df.plot(grid=True)

SERIES_IDS = (
    {'v21573668': 36100207},  # Not Useful: Real Gross Domestic Product
)

df = pd.concat(
    map(
        lambda _: combine_can_special(
            *dichotomize_series_ids(_, TO_PARSE_DATES)
        ),
        SERIES_IDS
    ),
    axis=1,
    sort=True
)

df.plot(grid=True)

SERIES_IDS = (
    {'v41713056': 36100208},  # Not Useful: Capital Input
    {'v41713073': 36100208},  # Not Useful: Capital Stock
    {'v41707775': 36100309},  # Not Useful: Capital Input
    {'v42189387': 36100310},  # Not Useful: Capital Input
)

df = pd.concat(
    map(
        lambda _: combine_can_special(
            *dichotomize_series_ids(_, TO_PARSE_DATES)
        ),
        SERIES_IDS
    ),
    axis=1,
    sort=True
)

df.plot(grid=True)

# =============================================================================
# Capital cost
# =============================================================================
SERIES_IDS = {
    'v41713243': 36100208,
    'v41708375': 36100309,
    'v42189907': 36100310,
}

df = stockpile_can(SERIES_IDS)
df['mean'] = df.mean(axis=1)
df.plot(grid=True)
df = df.iloc[:, [-1]]

# =============================================================================
# FILE_NAME = 'data_composed.csv'
# kwargs = {
#     'path_or_buf': BASE_DIR.joinpath(FILE_NAME)
# }
# df.to_csv(**kwargs)
# =============================================================================

df = pd.concat([stockpile_can(BLUEPRINT_CAPITAL), df], axis=1)


SERIES_IDS = {
    # =========================================================================
    # Manufacturing Indexes
    # =========================================================================
    'v86718697': 36100217,
    'v41707475': 36100309,
    'v42189127': 36100310,
    'v11567': 36100386
}

df = stockpile_can(SERIES_IDS)

SERIES_IDS = {
    # =========================================================================
    # Gross Output
    # =========================================================================
    'v86719219': 36100217,
    'v41708195': 36100309,
    'v42189751': 36100310,
    'v64602050': 36100488
}

df = stockpile_can(SERIES_IDS)

# =============================================================================
# FILE_NAME = 'data_composed.csv'
# kwargs = {
#     'path_or_buf': BASE_DIR.joinpath(FILE_NAME)
# }
# df.to_csv(**kwargs)
# =============================================================================


SERIES_IDS = {
    'v46445661': 36100210,
    'v46445722': 36100210,
    'v46445783': 36100210,
    'v46445844': 36100210,
    'v46445905': 36100210
}
df = stockpile_can(SERIES_IDS)
df['mean'] = df.sum(axis=1)
df = df.iloc[:, [-1]]


SERIES_IDS = {
    'v46444624': 36100210,
    'v46444685': 36100210,
    'v46444746': 36100210,
    'v46444990': 36100210,
    'v46445051': 36100210,
    'v46445112': 36100210,
    'v46445356': 36100210,
    'v46445417': 36100210,
    'v46445478': 36100210,
    'v46445722': 36100210,
    'v46445783': 36100210,
    'v46445844': 36100210,
}

for series_id in tuple(SERIES_IDS)[::3]:
    stockpile_can({series_id: SERIES_IDS[series_id]}).plot(grid=True)

for series_id in tuple(SERIES_IDS)[1::3]:
    stockpile_can({series_id: SERIES_IDS[series_id]}).plot(grid=True)

for series_id in tuple(SERIES_IDS)[2::3]:
    stockpile_can({series_id: SERIES_IDS[series_id]}).plot(grid=True)
