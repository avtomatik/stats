from itertools import combinations

import pandas as pd
from pandas import DataFrame

from stats.src.common.funcs import archive_name_to_url
from thesis.src.lib.read import read_can


def build_push_data_frame(path_or_buf: str, blueprint: dict) -> None:
    """
    Builds DataFrame & Loads It To CSV
    Parameters
    ----------
    path_or_buf : str
        Excel File Name.
    blueprint : dict
        DESCRIPTION.
    Returns
    -------
    None
    """
    # =========================================================================
    # TODO: Re-Write
    # =========================================================================

    df = DataFrame()
    for entry in blueprint:
        _df = read_can(archive_name_to_url(entry['archive_name']))
        _df = _df[_df['VECTOR'].isin(entry['series_ids'])]
        for series_id in entry['series_ids']:
            chunk = _df[_df['VECTOR'] == series_id][['VALUE']]
            chunk = chunk.groupby(chunk.index.year).mean()
            df = pd.concat([df, chunk], axis=1, sort=True)
        df.columns = entry['series_ids']
    df.to_csv(**kwargs)

    _df = pd.read_excel(path_or_buf, skiprows=range(1, 7), index_col=0)

    matches = []
    for pair in combinations(_df.columns, 2):
        chunk = _df.loc[:, list(pair)].dropna(axis=0)
        if (not chunk.empty) & chunk.iloc[:, 0].equals(chunk.iloc[:, 1]):
            matches.append(pair)
    for pair in matches:
        print(pair)
