from thesis.src.lib.stockpile import stockpile_can


def get_mean_for_min_std():
    """
    Determine Year & Mean Value for Base Vectors for Year with Minimum Standard Error
    """
    FILE_NAME = 'stat_can_lab.csv'
    # =========================================================================
    # Base Vectors
    # =========================================================================
    SERIES_IDS = {
        'v123355112': 14100355,
        'v1235071986': 14100392,
        'v2057609': 14100355,
        'v2057818': 14100355,
        'v2523013': 14100027,
    }

    df = stockpile_can(SERIES_IDS).dropna(axis=0)
    df['std'] = df.std(axis=1)
    return (
        df.iloc[:, [-1]].idxmin()[0],
        df.loc[df.iloc[:, [-1]].idxmin()[0], :][:-1].mean()
    )
