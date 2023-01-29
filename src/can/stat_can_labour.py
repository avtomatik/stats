# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import os
from pathlib import Path

import pandas as pd
from pandas import DataFrame

# =============================================================================
# Labor
# =============================================================================
# =============================================================================
# v41707595 # Not Useful: Labour input
# v41712954 # Not Useful: Labour input
# v42189231 # Not Useful: Labour input
# v65522120 # Not Useful
# v65522415 # Not Useful
# =============================================================================


def append_series_ids_sum(df: DataFrame, chunk: DataFrame, series_ids: tuple[str]) -> None:
    """


    Parameters
    ----------
    df : DataFrame
        DESCRIPTION.
    chunk : DataFrame
        DESCRIPTION.
    series_ids : tuple[str]
        DESCRIPTION.

    Returns
    -------
    None
        DESCRIPTION.

    """
    _chunk = pd.concat(
        [df.loc[:, (series_id,)].dropna(axis=0) for series_id in series_ids],
        axis=1
    )
    _chunk["_".join((*series_ids, "sum"))] = _chunk.sum(axis=1)
    return pd.concat(
        [
            chunk,
            _chunk.iloc[:, [-1]]
        ],
        axis=1
    )


result = DataFrame()
combined = DataFrame()
FILE_NAME = 'stat_can_prd.csv'
data = read_temporary(FILE_NAME)


SERIES_IDS = (
    'v716397',  # Total number of jobs
    'v718173',
    'v719421',  # Total number of jobs
)
combined = data.loc[:, SERIES_IDS].dropna(how="all")


SERIES_IDS = (
    'v535579',
    'v535593',
    'v535663',
    'v535677',
)
combined = append_series_ids_sum(data, combined, SERIES_IDS)


def shadow_append(df, series_ids):
    data = df.loc[:, series_ids].dropna(how="all")
    data["_".join((*series_ids, "sum"))] = data.sum(axis=1)
    return data


FILE_NAME = 'stat_can_lab.csv'
data = read_temporary(FILE_NAME)
SERIES_IDS = (
    'v74989',
    'v2057609',
    'v123355112',
    'v2057818',
)
combined = data.loc[:, SERIES_IDS].dropna(how="all")
combined = combined.div(combined.loc[1982]).mul(100)
combined['mean'] = combined.mean(axis=1)
result = pd.concat([result, combined.iloc[:, [-1]]], axis=1)


combined = DataFrame()
FILE_NAME = 'stat_can_prd.csv'
data = read_temporary(FILE_NAME)


SERIES_IDS = (
    'v21573686',  # Total number of jobs
    'v111382232',  # Total number of jobs
)
combined = data.loc[:, SERIES_IDS].dropna(how="all")
SERIES_IDS = (
    'v761808',
    'v761927',
)


combined = append_series_ids_sum(data, combined, SERIES_IDS)
FILE_NAME = 'stat_can_lab.csv'
data = read_temporary(FILE_NAME)


SERIES_IDS = (
    'v249139',
    'v2523013',
    'v1596771',
    'v78931172',
    'v65521825',
)
combined = data.loc[:, SERIES_IDS].dropna(how="all")


SERIES_IDS = (
    'v249703',
    'v250265',
)
combined = append_series_ids_sum(data, combined, SERIES_IDS)
SERIES_IDS = (
    'v78931174',
    'v78931173',
)
combined = append_series_ids_sum(data, combined, SERIES_IDS)
combined = combined.div(combined.loc[2000]).mul(100)
combined['mean'] = combined.mean(axis=1)
result = pd.concat([result, combined.iloc[:, [-1]]], axis=1)


combined = DataFrame()
FILE_NAME = 'stat_can_lab.csv'
data = read_temporary(FILE_NAME)
SERIES_IDS = (
    'v1235071986',
)
combined = data.loc[:, SERIES_IDS].dropna(how="all")
SERIES_IDS = (
    'v54027148',
    'v54027152',
)
combined = append_series_ids_sum(data, combined, SERIES_IDS)
combined = combined.div(combined.loc[2006]).mul(100)
combined['mean'] = combined.mean(axis=1)
result = pd.concat([result, combined.iloc[:, [-1]]], axis=1)


result = result.div(result.iloc[result.index.get_loc(2001), :]).mul(100)
result['mean_comb'] = result.mean(axis=1)
result = result.iloc[:, [-1]]
year, value = get_mean_for_min_std()
result['workers'] = result.div(result.loc[year, :]).mul(value)
result = result.iloc[:, [-1]].round(1)
result.plot(grid=True)
os.chdidr(DIR)
# .get_figure().savefig('view_canada.pdf', format='pdf', dpi=900)
# result.to_excel('result.xlsx')
#


FILE_NAME = 'series_ids.xlsx'
data = pd.read_excel(Path(DIR).joinpath(FILE_NAME))
data.dropna(axis=0, how='all', inplace=True)
data.dropna(axis=1, how='all', inplace=True)
# data.to_excel('series_ids.xlsx', index=False)

data.fillna('None', inplace=True)
version = sorted(data.iloc[:, 0].unique())[0]
chunk = data[data.iloc[:, 0] == version].iloc[:, 1:]
chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
initial = set(chunk.iloc[:, 0])
version = sorted(data.iloc[:, 0].unique())[2]
chunk = data[data.iloc[:, 0] == version].iloc[:, 1:]
chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
refactd = set(chunk.iloc[:, 0])
# for item in sorted(initial and refactd):
#     print(item)


FILE_NAME = 'stat_can_cap.csv'
data = pd.read_csv(Path(DIR).joinpath(FILE_NAME))
a = set(data.T.index)
a.remove('REF_DATE')
FILE_NAME = 'stat_can_lab.csv'
data = pd.read_csv(Path(DIR).joinpath(FILE_NAME))
b = set(data.T.index)
b.remove('REF_DATE')
FILE_NAME = 'stat_can_prd.csv'
data = pd.read_csv(Path(DIR).joinpath(FILE_NAME))
c = set(data.T.index)
c.remove('REF_DATE')


a -= refactd
b -= refactd
c -= refactd
