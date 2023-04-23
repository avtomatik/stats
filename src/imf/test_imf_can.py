# =============================================================================
# Convert CAD to USD
# =============================================================================


from pathlib import Path

import pandas as pd
from pandas import DataFrame

from stats.src.can.pull import pull_by_series_id

DIR = "/media/green-machine/KINGSTON"


def pull_imf_can_gdp_by_series_id(df: DataFrame, series_id: str) -> DataFrame:
    # =========================================================================
    # TODO: Refactor
    # =========================================================================
    chunk = df[df.iloc[:, 4] == series_id]
    chunk.columns = ('period', series_id)
    chunk = chunk.drop(
        chunk[chunk.iloc[:, 11] == 'Estimates Start After'].index)
    chunk = chunk.iloc[:, (11, 12)]
    chunk.iloc[:, 1] = chunk.iloc[:, 1].astype(float)
    return chunk.set_index('period')


def combine_imf_can_gdp_for_year_base(year_base: int) -> DataFrame:
    # =========================================================================
    # TODO: Refactor
    # =========================================================================
    FILE_NAME = "dataset_world_imf-WEOApr2018all.xls"
    SERIES_IDS = ['NGDP_R', 'NGDP', 'NGDPD', 'NGDP_D']
    kwargs = {
        "filepath_or_buffer": Path(DIR).joinpath(FILE_NAME),
        "low_memory": False
    }
    kwargs = {
        "filepath_or_buffer": 'dataset World IMF World Economic Outlook.csv',
        "low_memory": False
    }

    df = pd.read_csv(**kwargs)
    df = df[df.iloc[:, 1] ==
            f'International Monetary Fund, World Economic Outlook Database, April {year_base}']
    df = df[df.iloc[:, 3] == 'CAN']
    return pd.concat(
        map(lambda _: df.pipe(pull_imf_can_gdp_by_series_id, _), SERIES_IDS),
        axis=1,
        sort=True
    )


def read() -> DataFrame:
    ARCHIVE_ID = 3790031
    MAP = {
        'period': 0,
        'geo': 1,
        'seas': 2,
        'prices': 3,
        'naics': 4,
        'series_id': 5,
        'value': 7
    }
    kwargs = {
        'filepath_or_buffer': Path(DIR).joinpath(f'dataset_can_{ARCHIVE_ID:08n}-eng.zip'),
        'header': 0,
        'names': tuple(MAP.keys()),
        'index_col': 0,
        'usecols': tuple(MAP.values()),
        'parse_dates': ARCHIVE_ID in (2820011, 3790031, 3800084, 36100434)
    }
    return pd.read_csv(**kwargs)


def filter_df(df: DataFrame) -> DataFrame:
    FILTER = (
        (df.loc[:, 'naics'] == 'All industries (x 1,000,000)') &
        (df.loc[:, 'series_id'] != 'v65201756')
    )
    FILTER = (
        (df.loc[:, 'naics'] == 'Manufacturing (x 1,000,000)') &
        (df.loc[:, 'series_id'] != 'v65201809')
    )
    return df[FILTER].iloc[:, -2:]


_df = read().pipe(filter_df)
# =============================================================================
# Kludge
# =============================================================================
_df.iloc[:, -1] = _df.iloc[:, -1].apply(pd.to_numeric, errors='coerce')


df = pd.concat(
    map(
        lambda _: _df.pipe(pull_by_series_id, _),
        sorted(set(_df.loc[:, "series_id"]))
    ),
    axis=1,
    sort=True
)
df = df.groupby(df.index.year).mean()
df['def'] = df.iloc[:, 0].div(df.iloc[:, 1])
df = df.div(df.loc[2012, :])
df['real_rebased'] = df.iloc[:, 1].mul(df.iloc[:, -1])

file_name = 'dataset_can_CANSIM.csv'
df.to_csv(file_name)

df = combine_imf_can_gdp_for_year_base(2015)
