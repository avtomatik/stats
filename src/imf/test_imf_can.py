# =============================================================================
# Convert CAD to USD
# =============================================================================


from typing import Any

import pandas as pd
from core.config import DATA_DIR

from stats.src.can.pull import pull_by_series_id


def pull_imf_can_gdp_by_series_id(df: pd.DataFrame, series_id: str) -> pd.DataFrame:
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


def combine_imf_can_gdp_for_year_base(year_base: int) -> pd.DataFrame:
    # =========================================================================
    # TODO: Refactor
    # =========================================================================

    SERIES_IDS = ['NGDP_R', 'NGDP', 'NGDPD', 'NGDP_D']

    df = pd.read_csv(**get_kwargs_imf_gdp())
    df = df[
        df.iloc[:,
                1] == f'International Monetary Fund, World Economic Outlook Database, April {year_base}'
    ]
    df = df[df.iloc[:, 3] == 'CAN']
    return pd.concat(
        map(lambda _: df.pipe(pull_imf_can_gdp_by_series_id, _), SERIES_IDS),
        axis=1,
        sort=True
    )


def get_kwargs_imf_gdp():

    FILE_NAME = "dataset_world_imf-WEOApr2018all.xls"

    kwargs = {
        'filepath_or_buffer': DATA_DIR.joinpath(FILE_NAME),
        "low_memory": False
    }

    FILE_NAME = 'dataset World IMF World Economic Outlook.csv'
    return {
        'filepath_or_buffer': DATA_DIR.joinpath(FILE_NAME),
        "low_memory": False
    }


def get_kwargs_can() -> dict[str, Any]:

    ARCHIVE_ID = 3790031
    NAMES = ['period', 'geo', 'seas', 'prices', 'naics', 'series_id', 'value']
    USECOLS = [0, 1, 2, 3, 4, 5, 7]

    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )

    FILE_NAME = f'dataset_can_{ARCHIVE_ID:08n}-eng.zip'
    return {
        'filepath_or_buffer': DATA_DIR.joinpath(FILE_NAME),
        'header': 0,
        'names': NAMES,
        'index_col': 0,
        'usecols': USECOLS,
        'parse_dates': ARCHIVE_ID in TO_PARSE_DATES
    }


def filter_df(df: pd.DataFrame) -> pd.DataFrame:
    FILTER = (
        (df.loc[:, 'naics'] == 'All industries (x 1,000,000)') &
        (df.loc[:, 'series_id'] != 'v65201756')
    )
    FILTER = (
        (df.loc[:, 'naics'] == 'Manufacturing (x 1,000,000)') &
        (df.loc[:, 'series_id'] != 'v65201809')
    )
    return df[FILTER].iloc[:, -2:]


_df = pd.read_csv(**get_kwargs_can()).pipe(filter_df)
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

file_name = 'dataset_can_cansim.csv'
df.to_csv(file_name)

df = combine_imf_can_gdp_for_year_base(2015)
