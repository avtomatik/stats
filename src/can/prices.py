import pandas as pd
from thesis.src.lib.tools import construct_usa_hist_deflator

from stats.src.can.combine import combine_can_price_a, combine_can_price_b
from stats.src.can.constants import (SERIES_IDS_CD, SERIES_IDS_PRCH,
                                     SERIES_IDS_PRICE_A, SERIES_IDS_PRICE_B)

df = pd.concat(
    [
        pd.concat(
            map(construct_usa_hist_deflator, (SERIES_IDS_CD, SERIES_IDS_PRCH))
        ),
        combine_can_price_a(SERIES_IDS_PRICE_A),
        combine_can_price_b(SERIES_IDS_PRICE_B),
    ],
    axis=1,
    sort=True
)

df['mean'] = df.mean(axis=1)
df['cum_mean'] = df.iloc[:, -1].add(1).cumprod()
df = df.div(df.loc[2012])
FILE_NAME = 'data_composed.csv'
df.to_csv(FILE_NAME)
