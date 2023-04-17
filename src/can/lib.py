import pandas as pd
from pandas import DataFrame

from thesis.src.lib.collect import stockpile_can
from thesis.src.lib.transform import transform_deflator, transform_pct_change


def combine_can_price_a() -> DataFrame:
    SERIES_IDS = (
        {
            'v90968617': 36100096,
            'v90971177': 36100096
        },
        {
            'v90968618': 36100096,
            'v90971178': 36100096
        },
        {
            'v90968619': 36100096,
            'v90971179': 36100096
        },
        {
            'v90968620': 36100096,
            'v90971180': 36100096
        },
        {
            'v90968621': 36100096,
            'v90971181': 36100096
        },
        {
            'v1071434': 36100236,
            'v1119722': 36100236
        },
        {
            'v1071435': 36100236,
            'v1119723': 36100236
        },
        {
            'v1071436': 36100236,
            'v1119724': 36100236
        },
        {
            'v1071437': 36100236,
            'v1119725': 36100236
        }
    )
    # =============================================================================
    #     SERIES_IDS = (
    #         {
    #             'v90968617': 36100096,
    #             'v90973737': 36100096
    #         },
    #         {
    #             'v90968618': 36100096,
    #             'v90973738': 36100096
    #         },
    #         {
    #             'v90968619': 36100096,
    #             'v90973739': 36100096
    #         },
    #         {
    #             'v90968620': 36100096,
    #             'v90973740': 36100096
    #         },
    #         {
    #             'v90968621': 36100096,
    #             'v90973741': 36100096
    #         },
    #         {
    #             'v1071434': 36100236,
    #             'v4421025': 36100236
    #         },
    #         {
    #             'v1071435': 36100236,
    #             'v4421026': 36100236
    #         },
    #         {
    #             'v1071436': 36100236,
    #             'v4421027': 36100236
    #         },
    #         {
    #             'v1071437': 36100236,
    #             'v4421028': 36100236
    #         },
    #         {
    #             'v64498363': 36100236,
    #             'v64498379': 36100236
    #         }
    #     )
    # =============================================================================
    return pd.concat(
        map(lambda _: stockpile_can(_).pipe(transform_deflator), SERIES_IDS),
        axis=1,
        sort=True
    )


def combine_can_price_b():
    SERIES_IDS = (
        {'v46444990': 36100210},
        {'v46445051': 36100210},
        {'v46445112': 36100210}
    )
    return pd.concat(
        map(lambda _: stockpile_can(_).pipe(transform_pct_change), SERIES_IDS), axis=1
    )


def get_mean_for_min_std():
    """
    Determine Year & Mean Value for Base Vectors for Year with Minimum StandardError
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