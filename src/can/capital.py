# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import io
from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import requests
from pandas import DataFrame


# =============================================================================
# TODO: Clear It Up
# =============================================================================
# =============================================================================
# TODO: Extend
# =============================================================================
MAP_READ_CAN_SPC = {
    10100094: {},
    14100027: {
        'period': 0,  # int64
        'series_id': 10,  # object
        'value': 12,  # float64
    },
    14100221: {
        'period': 0,  # object
        'series_id': 10,  # object
        'value': 12,  # float64
    },
    14100235: {
        'period': 0,  # object
        'series_id': 8,  # object
        'value': 10,  # float64
    },
    14100238: {
        'period': 0,  # object
        'series_id': 8,  # object
        'value': 10,  # float64
    },
    14100243: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    14100265: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    14100355: {
        'period': 0,  # object
        'series_id': 10,  # object
        'value': 12,  # float64
    },
    14100392: {
        'period': 0,  # int64
        'series_id': 8,  # object
        'value': 10,  # float64
    },
    16100053: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    16100054: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    16100109: {},
    16100111: {},
    36100207: {
        'period': 0,  # object
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100208: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100210: {},
    36100217: {},
    36100303: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100305: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100309: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100310: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100386: {},
    36100480: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
    36100488: {},
    36100489: {
        'period': 0,  # int64
        'series_id': 9,  # object
        'value': 11,  # float64
    },
}


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
            kwargs['filepath_or_buffer'] = ZipFile(io.BytesIO(
                requests.get(url).content)
            ).open(f'{archive_id:08n}.csv')
    return pd.read_csv(**kwargs)


def pull_by_series_id(df: DataFrame, series_id: str) -> DataFrame:
    """


    Parameters
    ----------
    df : DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series IDs
        df.iloc[:, 1]      Values
        ================== =================================
    series_id : str

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Series
        ================== =================================
    """
    assert df.shape[1] == 2
    return df[df.iloc[:, 0] == series_id].iloc[:, [1]].rename(
        columns={"value": series_id}
    )


def stockpile_can(series_ids: dict[str, int]) -> pd.DataFrame:
    """
    Parameters
    ----------
    series_ids : dict[str, int]
        DESCRIPTION.
    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    return pd.concat(
        map(
            lambda _: read_can(_[1]).pipe(pull_by_series_id, _[0]),
            series_ids.items()
        ),
        axis=1,
        sort=True
    )


def transform_year_mean(df: DataFrame) -> DataFrame:
    return df.groupby(df.index.year).mean()


def read_temporary(
    file_name: str, path_src: str = '/home/green-machine/data_science/data/interim'
) -> DataFrame:
    """


    Parameters
    ----------
    file_name : str
        DESCRIPTION.
    path_src : str, optional
        DESCRIPTION. The default is '/home/green-machine/data_science/data/interim'.

    Returns
    -------
    DataFrame
        DESCRIPTION.

    """
    kwargs = {
        'filepath_or_buffer': Path(path_src).joinpath(file_name),
        'index_col': 0,
    }
    return pd.read_csv(**kwargs)


PATH_EXPORT = '/home/green-machine/Downloads'

# =============================================================================
# Product
# =============================================================================

SERIES_IDS = {
    'v21573668': 36100207,  # Not Useful: Real Gross Domestic Product
    'v142817': 16100111,  # Not Useful: Capacity Utilization
    'v37482': 10100094,  # Not Useful: Capacity Utilization
    'v4331088': 16100109,  # Not Useful: Capacity Utilization
    'v41713056': 36100208,  # Not Useful: Capital Input
    'v41713073': 36100208,  # Not Useful: Capital Input
    'v41707775': 36100309,  # Not Useful: Capital Input
    'v42189387': 36100310,  # Not Useful: Capital Input
}

df = DataFrame()
combined = DataFrame()
# =============================================================================
# Capital cost
# =============================================================================
SERIES_IDS = {
    'v41713243': 36100208,
    'v41708375': 36100309,
    'v42189907': 36100310,
}

combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)
combined = combined.div(combined.loc[1997]).mul(100)
combined['mean_comb'] = combined.mean(axis=1)
combined = combined.iloc[:, [-1]]
df = pd.concat([df, combined], axis=1)

file_name = 'df.csv'
# =============================================================================
# combined.to_csv(Path(PATH_EXPORT).joinpath(file_name))
# =============================================================================

FILE_NAME = 'stat_can_cap.csv'
data = read_temporary(FILE_NAME)
data = data.div(data.loc[1997]).mul(100)
combined = pd.concat([data, combined], axis=1)

data = combined


combined = DataFrame()

SERIES_IDS = {
    # =========================================================================
    # Manufacturing Indexes
    # =========================================================================
    'v86718697': 36100217,
    'v41707475': 36100309,
    'v42189127': 36100310,
    'v11567': 36100386
}

combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)
combined = combined.div(combined.loc[1961]).mul(100)

SERIES_IDS = {
    # =========================================================================
    # Gross Output
    # =========================================================================
    'v86719219': 36100217,
    'v41708195': 36100309,
    'v42189751': 36100310,
    'v64602050': 36100488
}

combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)
combined = combined.div(combined.loc[1997]).mul(100)

file_name = 'df.csv'
# =============================================================================
# combined.to_csv(Path(PATH_EXPORT).joinpath(file_name))
# =============================================================================


FILE_NAME = 'stat_can_cap.csv'
data = read_temporary(FILE_NAME)
combined = pd.concat(
    map(lambda _: data.iloc[:, [_]].dropna(axis=0), range(30, 35)),
    axis=1
)
combined = combined.div(combined.loc[1997]).mul(100)
combined['mean'] = combined.sum(axis=1)
combined = combined.iloc[:, [-1]]
df = pd.concat([df, combined], axis=1)

FILE_NAME = 'stat_can_cap_matching.csv'
data = read_temporary(FILE_NAME)
data = data[data.iloc[:, 5] !=
            'Information and communication technologies machinery and equipment']
data = data[data.iloc[:, 5] != 'Land']
data = data[data.iloc[:, 6] != 'Intellectual property products']
# =============================================================================
# data.dropna(axis=0, how='all').to_csv(
#     Path(PATH_EXPORT).joinpath(FILE_NAME), index=True
# )
# =============================================================================

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
