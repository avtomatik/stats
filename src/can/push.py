import pandas as pd
from pandas import DataFrame
from thesis.src.lib.read import read_can
from thesis.src.lib.tools import archive_name_to_url


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
    df = DataFrame()
    for entry in blueprint:
        _df = read_can(archive_name_to_url(entry['archive_name']))
        _df = _df[_df['VECTOR'].isin(entry['series_ids'])]
        for series_id in entry['series_ids']:
            chunk = _df[_df['VECTOR'] == series_id][['VALUE']]
            chunk = chunk.groupby(chunk.index.year).mean()
            df = pd.concat([df, chunk], axis=1, sort=True)
        df.columns = entry['series_ids']
    df.to_csv(path_or_buf)
