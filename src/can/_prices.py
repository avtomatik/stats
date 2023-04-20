import pandas as pd
from pandas import DataFrame

from stats.src.can.combine import combine_can_price_a, combine_can_price_b
from stats.src.can.constants import SERIES_IDS_CD, SERIES_IDS_PRCH
from thesis.src.lib.tools import construct_usa_hist_deflator

df = pd.concat(
    map(construct_usa_hist_deflator, (SERIES_IDS_CD, SERIES_IDS_PRCH)),
    axis=1,
    sort=True
)

df = DataFrame()

CALLS = {
    'cobb_douglas': construct_usa_hist_deflator(SERIES_IDS_CD),
    'uscb': construct_usa_hist_deflator(SERIES_IDS_PRCH),
    'canada_a': combine_can_price_a(),
    'canada_b': combine_can_price_b(),
}

for key, chunk in CALLS.items():
    df = pd.concat([df, chunk], axis=1, sort=False)
df['mean'] = df.mean(axis=1)
df['cum_mean'] = df.iloc[:, -1].add(1).cumprod()
df = df.div(df.loc[2012])
df.to_excel('basis_frame.xlsx')

CALLS = (
    construct_usa_hist_deflator(SERIES_IDS_CD),
    construct_usa_hist_deflator(SERIES_IDS_PRCH),
    combine_can_price_a,
    combine_can_price_b,
)

df = pd.concat([call() for call in CALLS], axis=1)
df['mean'] = df.mean(axis=1)
df['cum_mean'] = df.iloc[:, -1].add(1).cumprod()
df = df.div(df.loc[2012])
