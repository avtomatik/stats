import pandas as pd

from stats.src.common.transform import transform_year_mean
from thesis.src.lib.stockpile import stockpile_can


def get_mean_for_min_std() -> tuple[int, float]:
    """
    Determine Year & Mean Value for Base Series IDs for Year with Minimum Standard Error

    Returns
    -------
    tuple[int, float]
        DESCRIPTION.

    """
    FILE_NAME = 'stat_can_lab.csv'
    # =========================================================================
    # Base Series IDs
    # =========================================================================
    SERIES_IDS_NO_OPERATION = {'v2523013': 14100027, 'v1235071986': 14100392}
    SERIES_IDS_TO_TRANSFORM = {
        'v123355112': 14100355, 'v2057609': 14100355, 'v2057818': 14100355
    }
    df = pd.concat(
        [
            stockpile_can(SERIES_IDS_NO_OPERATION),
            stockpile_can(SERIES_IDS_TO_TRANSFORM).pipe(transform_year_mean)
        ],
        axis=1,
        sort=True
    ).dropna(axis=0)
    df['std'] = df.std(axis=1)
    year_min_std = int(df.iloc[:, [-1]].idxmin()[0])
    return year_min_std, df.loc[year_min_std, :][:-1].mean()
