

import pandas as pd
from common.funcs import get_pre_kwargs

PATH_EXPORT = '/home/green-machine/Downloads'
FILE_NAME = 'stat_can_cap_matching.csv'

df = pd.read_csv(**get_pre_kwargs(FILE_NAME))

CRITERIA = [
    'Information and communication technologies machinery and equipment',
    'Land'
]
df = df[~df.loc[:, 'desc_1'].isin(CRITERIA)]
CRITERIA = ['Intellectual property products']
df = df[~df.loc[:, 'desc_2'].isin(CRITERIA)]

# =============================================================================
# df.dropna(axis=0, how='all').to_csv(
#     Path(PATH_EXPORT).joinpath(FILE_NAME), index=True
# )
# =============================================================================
