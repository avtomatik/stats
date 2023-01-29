# =============================================================================
# Convert CAD to USD
# =============================================================================


from pathlib import Path

import pandas as pd
from pandas import DataFrame

# =============================================================================
# from lib.pull import pull_by_series_id
# =============================================================================

DIR = "/media/green-machine/KINGSTON"


def pull_imf_can_gdp_by_series_id(df, series_id):
    # =========================================================================
    # TODO: Refactor
    # =========================================================================
    chunk = df[df.iloc[:, 4] == series_id]
    chunk.columns = ('period', series_id)
    chunk = chunk.drop(
        chunk[chunk.iloc[:, 11] == 'Estimates Start After'].index)
    chunk = chunk.iloc[:, (11, 12)]
    chunk.iloc[:, 1] = chunk.iloc[:, 1].astype(float)
    chunk = chunk.set_index('period')
    return chunk


def collect_imf_can_gdp_for_year_base(year_base: int) -> DataFrame:
    # =========================================================================
    # TODO: Refactor
    # =========================================================================
    FILE_NAME = "dataset_world_imf-WEOApr2018all.xls"
    kwargs = {
        "filepath_or_buffer": 'dataset World IMF World Economic Outlook.csv',
        # "filepath_or_buffer": Path(DIR).joinpath(FILE_NAME),
        "low_memory": False
    }

    df = pd.read_csv(**kwargs)
    df = df[df.iloc[:, 1] ==
            'International Monetary Fund, World Economic Outlook Database, April %d' % (year_base,)]
    df = df[df.iloc[:, 3] == 'CAN']
    return pd.concat(
        [
            pull_imf_can_gdp_by_series_id(df, 1, 'NGDP_R'),
            pull_imf_can_gdp_by_series_id(df, 2, 'NGDP'),
            pull_imf_can_gdp_by_series_id(df, 3, 'NGDPD'),
            pull_imf_can_gdp_by_series_id(df, 4, 'NGDP_D')
        ],
        axis=1,
        sort=True
    )


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
    [
        _df.pipe(pull_by_series_id, series_id)
        for series_id in sorted(set(_df.loc[:, "series_id"]))
    ],
    axis=1,
    sort=True
)
df = df.groupby(df.index.year).mean()
df['def'] = df.iloc[:, 0].div(df.iloc[:, 1])
df = df.div(df.loc[2012, :])
df['real_rebased'] = df.iloc[:, 1].mul(df.iloc[:, -1])

df.to_csv('dataset_can_CANSIM.csv')

df = collect_imf_can_gdp_for_year_base(2015)
