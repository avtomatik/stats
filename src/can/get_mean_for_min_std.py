import pandas as pd
from stats.src.common.funcs import dichotomize_series_ids


def get_mean_for_min_std(
    series_ids_reference: tuple[dict[str, int]],
    source_ids: tuple[int]
) -> tuple[int, float]:
    """
    Determine Year & Mean Value for Base Series IDs for Year with Minimum Standard Error

    Parameters
    ----------
    series_ids_reference : tuple[dict[str, int]]
        DESCRIPTION.

    Returns
    -------
    tuple[int, float]
        DESCRIPTION.

    """
    FILE_NAME = 'stat_can_lab.csv'
    # =========================================================================
    # Base Series IDs
    # =========================================================================
    df = pd.concat(
        map(
            lambda _: combine_can_special(
                *dichotomize_series_ids(_, source_ids)
            ),
            series_ids_reference
        ),
        axis=1,
        sort=True
    ).dropna(axis=0)
    df['std'] = df.std(axis=1)
    year_min_std = int(df.iloc[:, [-1]].idxmin()[0])
    return year_min_std, df.loc[year_min_std, :][:-1].mean()
