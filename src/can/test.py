import pandas as pd

from thesis.src.lib.plot import plot_can_test
from thesis.src.lib.pull import pull_by_series_id
from thesis.src.lib.read import read_can
from thesis.src.lib.transform import transform_year_sum


def test_data_can():
    """Project I: Canada Gross Domestic Product Data Comparison"""
    # =========================================================================
    # Expenditure-Based Gross Domestic Product Series Used
    # Income-Based Gross Domestic Product Series Not Used
    # =========================================================================
    SERIES_IDS = {
        # =====================================================================
        # Series A: Equals Series D, However, Series D Is Preferred Over Series A As It Is Yearly:
        # v62307282 - 380-0066 Price indexes, gross domestic product; Canada; Implicit price indexes; Gross domestic product at market prices (quarterly, 1961-03-01 to 2017-09-01)
        # =====================================================================
        'v62307282': 3800066,
        # =====================================================================
        # Series B: Equals Both Series C & Series E, However, Series E Is Preferred Over Both Series B & Series C As It Is Yearly: v62306896 - 380-0084 Gross domestic product at 2007 constant prices, expenditure-based; Canada; Seasonally adjusted at annual rates; Gross domestic product at market prices (x 1,000,000) (quarterly, 1961-03-01 to 2017-09-01)
        # =====================================================================
        'v62306896': 3800084,
        # =====================================================================
        # Series C: Equals Both Series B & Series E, However, Series E Is Preferred Over Both Series B & Series C As It Is Yearly: v62306938 - 380-0084 Gross domestic product at 2007 constant prices, expenditure-based; Canada; Unadjusted; Gross domestic product at market prices (x 1,000,000) (quarterly, 1961-03-01 to 2017-09-01)
        # =====================================================================
        'v62306938': 3800084,
        # =====================================================================
        # Series D: Equals Series A, However, Series D Is Preferred Over Series A As It Is Yearly: v62471023 - 380-0102 Gross domestic product indexes; Canada; Implicit price indexes; Gross domestic product at market prices (annual, 1961 to 2016)
        # =====================================================================
        'v62471023': 3800102,
        # =====================================================================
        # Series E: Equals Both Series B & Series C, However, Series E Is Preferred Over Both Series B & Series C As It Is Yearly: v62471340 - 380-0106 Gross domestic product at 2007 constant prices, expenditure-based; Canada; Gross domestic product at market prices (x 1,000,000) (annual, 1961 to 2016)
        # =====================================================================
        'v62471340': 3800106,
        'v96411770': 3800518,
        'v96391932': 3800566,
        'v96730304': 3800567,
        'v96730338': 3800567
    }
    df = pd.concat(
        [
            pd.concat(
                [
                    read_can(_args[0]).pipe(transform_year_sum, _args[1])
                    for _args in SERIES_IDS[:3]
                ],
                axis=1,
                sort=True
            ),
            pd.concat(
                [
                    read_can(_args[0]).pipe(pull_by_series_id, _args[1])
                    for _args in SERIES_IDS[3:]
                ],
                axis=1,
                sort=True
            ).apply(pd.to_numeric, errors='coerce'),
        ],
        axis=1,
        sort=True
    ).dropna(axis=0)
    df['series_0x0'] = df.iloc[:, 0].div(df.iloc[0, 0])
    df['series_0x1'] = df.iloc[:, 4].div(df.iloc[0, 4])
    df['series_0x2'] = df.iloc[:, 5].div(df.iloc[0, 5])
    df['series_0x3'] = df.iloc[:, 7].div(
        df.iloc[:, 6]).div(df.iloc[:, 5]).mul(100)
    df['series_0x4'] = df.iloc[:, 8].div(df.iloc[0, 8])
    # =========================================================================
    # Option 1
    # =========================================================================
    df.iloc[:, (-5, -3)].pipe(plot_can_test)
    # =========================================================================
    # Option 2
    # =========================================================================
    df.iloc[:, (-2, -1)].pipe(plot_can_test)
    # =========================================================================
    # Option 3
    # =========================================================================
    df.iloc[:, (-4, -1)].pipe(plot_can_test)

    # =========================================================================
    # Option 4: What?
    # =========================================================================
    # plot_can_test(df.iloc[:, -1].div(df.iloc[:, -1]), df.iloc[:, -3])
