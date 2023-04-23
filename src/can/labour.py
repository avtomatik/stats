# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import os
from pathlib import Path

import pandas as pd
from stats.src.lib.transform import transform_sum, transform_year_mean

from stats.src.can.get_mean_for_min_std import get_mean_for_min_std
from stats.src.can.stockpile import stockpile_can

# =============================================================================
# Labor
# =============================================================================
SERIES_IDS = {
    '!v41707595': 36100309,  # Not Useful: Labour input
    '!v41712954': 36100208,  # Not Useful: Labour input
    '!v42189231': 36100310,  # Not Useful: Labour input
    '!v65522120': 36100489,  # Not Useful
    '!v65522415': 36100489,  # Not Useful
}
path_src = '../data/external'


os.chdir(path_src)


SERIES_IDS = {
    'v716397': 36100303,  # Total number of jobs
    'v718173': 36100303,
    'v719421': 36100305,  # Total number of jobs
}

combined = stockpile_can(SERIES_IDS)


SERIES_IDS = {
    'v535579': 16100053,
    'v535593': 16100053,
    'v535663': 16100053,
    'v535677': 16100053,
}

combined = pd.concat(
    [
        combined,
        stockpile_can(SERIES_IDS).pipe(
            transform_sum, name='_'.join((*SERIES_IDS, 'sum')))
    ],
    axis=1
)


SERIES_IDS = {
    'v74989': 14100235,
    'v2057609': 14100355,
    'v123355112': 14100355,
    'v2057818': 14100355,
}

combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)
combined = combined.div(combined.loc[1982]).mul(100)
combined['mean'] = combined.mean(axis=1)
result = combined.iloc[:, [-1]]


SERIES_IDS = {
    'v21573686': 36100207,  # Total number of jobs
    'v111382232': 36100480,  # Total number of jobs
}


print(stockpile_can(SERIES_IDS).info())
combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)
print(combined)

SERIES_IDS = {
    'v761808': 16100054,
    'v761927': 16100054,
}


combined = pd.concat(
    [
        combined,
        stockpile_can(SERIES_IDS).pipe(
            transform_sum, name='_'.join((*SERIES_IDS, 'sum')))
    ],
    axis=1
)


SERIES_IDS = {
    'v249139': 14100265,
    'v2523013': 14100027,
    'v1596771': 14100238,
    'v78931172': 14100243,
    'v65521825': 36100489
}

combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)


SERIES_IDS = {
    'v249703': 14100265,
    'v250265': 14100265,
}

combined = pd.concat(
    [
        combined,
        stockpile_can(SERIES_IDS).pipe(
            transform_sum, name='_'.join((*SERIES_IDS, 'sum')))
    ],
    axis=1
)

SERIES_IDS = {
    'v78931174': 14100243,
    'v78931173': 14100243,
}

combined = pd.concat(
    [
        combined,
        stockpile_can(SERIES_IDS).pipe(
            transform_sum, name='_'.join((*SERIES_IDS, 'sum')))
    ],
    axis=1
)
combined = combined.div(combined.loc[2000]).mul(100)
combined['mean'] = combined.mean(axis=1)
result = pd.concat([result, combined.iloc[:, [-1]]], axis=1)


SERIES_IDS = {
    'v1235071986': 14100392,
}

combined = stockpile_can(SERIES_IDS).pipe(transform_year_mean)

SERIES_IDS = {
    'v54027148': 14100221,
    'v54027152': 14100221,
}

combined = pd.concat(
    [
        combined,
        stockpile_can(SERIES_IDS).pipe(
            transform_sum, name='_'.join((*SERIES_IDS, 'sum')))
    ],
    axis=1
)
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
os.chdir(path_export)
# =============================================================================
# .get_figure().savefig('view_canada.pdf', format='pdf', dpi=900)
# result.to_excel('result.xlsx')
# =============================================================================


FILE_NAME = 'series_ids.xlsx'
data = pd.read_excel(Path(path_src).joinpath(FILE_NAME))
data.dropna(axis=0, how='all', inplace=True)
data.dropna(axis=1, how='all', inplace=True)
# =============================================================================
# data.to_excel('series_ids.xlsx', index=False)
# =============================================================================

data.fillna('None', inplace=True)
version = sorted(data.iloc[:, 0].unique())[0]
chunk = data[data.iloc[:, 0] == version].iloc[:, 1:]
chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
SERIES_IDS_INIT = set(chunk.iloc[:, 0])
version = sorted(data.iloc[:, 0].unique())[2]
chunk = data[data.iloc[:, 0] == version].iloc[:, 1:]
chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
SERIES_IDS_FINAL = set(chunk.iloc[:, 0])
# =============================================================================
# for series_id in sorted(SERIES_IDS_INIT and SERIES_IDS_FINAL):
#     print(series_id)
# =============================================================================

# =============================================================================
# Test Series IDS
# =============================================================================
FILE_NAME = 'stat_can_cap.csv'
kwargs = {
    'filepath_or_buffer': Path(path_src).joinpath(FILE_NAME),
    'index_col': 0
}
SERIES_IDS_CAP = set(pd.read_csv(**kwargs).columns)

FILE_NAME = 'stat_can_lab.csv'
kwargs = {
    'filepath_or_buffer': Path(path_src).joinpath(FILE_NAME),
    'index_col': 0
}
SERIES_IDS_LAB = set(pd.read_csv(**kwargs).columns)

FILE_NAME = 'stat_can_prd.csv'
kwargs = {
    'filepath_or_buffer': Path(path_src).joinpath(FILE_NAME),
    'index_col': 0
}
SERIES_IDS_PRD = set(pd.read_csv(**kwargs).columns)

SERIES_IDS_CAP -= SERIES_IDS_FINAL
SERIES_IDS_LAB -= SERIES_IDS_FINAL
SERIES_IDS_PRD -= SERIES_IDS_FINAL
