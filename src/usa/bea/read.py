from pathlib import Path

import pandas as pd
from pandas import DataFrame

# =============================================================================
# Bureau of Economic Analysis
# =============================================================================


def read(file_name: str, path_src: str) -> DataFrame:

    filepath_or_buffer = Path(path_src).joinpath(file_name)

    return pd.read_csv(**get_kwargs(filepath_or_buffer))


def get_kwargs(filepath_or_buffer):

    NAMES = ['source_id', 'series_id', 'period', 'subperiod', 'value']
    USECOLS = [0, 14, 15, 16, 17]

    return {
        'filepath_or_buffer': filepath_or_buffer,
        'header': 0,
        'names': NAMES,
        'index_col': 2,
        'usecols': USECOLS,
    }


def read_usa_bea_pull_by_series_id(df: DataFrame, series_id: str) -> DataFrame:
    """
    Retrieve Yearly Data for BEA Series ID
    """
    df = df[df.loc[:, 'series_id'] == series_id]
    source_ids = sorted(set(df.loc[:, 'source_id']))
    chunk = pd.concat(
        map(
            lambda _: df[
                df.loc[:, 'source_id'] == _
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
