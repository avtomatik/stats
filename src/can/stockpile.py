import pandas as pd
from pandas import DataFrame

from stats.src.can.pull import pull_by_series_id
from stats.src.can.read import read_can


def stockpile_can(series_ids: dict[str, int]) -> DataFrame:
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
            lambda _: read_can(_[1]).pipe(
                pull_by_series_id, _[0]
            ),
            series_ids.items()
        ),
        axis=1,
        sort=True
    )
