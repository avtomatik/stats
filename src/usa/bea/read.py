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


def read_usa_bea_pull_by_series_id(df: DataFrame, series_id: str) -> DataFrame:
    """
    Retrieve Yearly Data for BEA Series ID
    """
    df = df[df.loc[:, "series_id"] == series_id]
    source_ids = sorted(set(df.loc[:, "source_id"]))
    chunk = pd.concat(
        map(
            lambda _: df[
                df.loc[:, "source_id"] == _
            ].iloc[:, [-1]].drop_duplicates(),
            source_ids
        ),
        axis=1,
        sort=True
    )
    chunk.columns = map(
        lambda _: ''.join((_.split()[1].replace('.', '_'), series_id)),
        source_ids
    )
    return chunk
