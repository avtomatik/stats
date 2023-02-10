# -*- coding: utf-8 -*-
"""
Created on Tue Nov  2 21:10:29 2021

@author: Alexander Mikhailov
"""


import os
from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from lib.collect import get_mean_for_min_std
from lib.pull import pull_by_series_id
from lib.read import read_temporary
from lib.transform import transform_sum
from pandas import DataFrame

# =============================================================================
# Labor
# =============================================================================
# =============================================================================
# {'!v41707595': 36100309} # Not Useful: Labour input
# {'!v41712954': 36100208} # Not Useful: Labour input
# {'!v42189231': 36100310} # Not Useful: Labour input
# {'!v65522120': 36100489} # Not Useful
# {'!v65522415': 36100489} # Not Useful
# =============================================================================
path_src = '/home/green-machine/data_science/data/external'


@cache
def read_can(archive_id: int) -> DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = {'period': 0, 'series_id': 10, 'value': 12}
    MAP = {
        310004: {
            'period': 0,
            'prices': 2,
            'category': 4,
            'component': 5,
            'series_id': 6,
            'value': 8
        },
        2820011: {
            'period': 0,
            'geo': 1,
            'classofworker': 2,
            'industry': 3,
            'sex': 4,
            'series_id': 5,
            'value': 7
        },
        2820012: {'period': 0, 'series_id': 5, 'value': 7},
        3790031: {
            'period': 0,
            'geo': 1,
            'seas': 2,
            'prices': 3,
            'naics': 4,
            'series_id': 5,
            'value': 7
        },
        3800084: {
            'period': 0,
            'geo': 1,
            'seas': 2,
            'est': 3,
            'series_id': 4,
            'value': 6
        },
        3800102: {'period': 0, 'series_id': 4, 'value': 6},
        3800106: {'period': 0, 'series_id': 3, 'value': 5},
        3800518: {'period': 0, 'series_id': 4, 'value': 6},
        3800566: {'period': 0, 'series_id': 3, 'value': 5},
        3800567: {'period': 0, 'series_id': 4, 'value': 6},
        36100096: {
            'period': 0,
            'geo': 1,
            'prices': 3,
            'industry': 4,
            'category': 5,
            'component': 6,
            'series_id': 11,
            'value': 13
        },
        36100303: {'period': 0, 'series_id': 9, 'value': 11},
        36100305: {'period': 0, 'series_id': 9, 'value': 11},
        16100053: {'period': 0, 'series_id': 9, 'value': 11},
    }
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'
    kwargs = {
        'header': 0,
        'names': list(MAP.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in (2820011, 3790031, 3800084, 36100108, 36100434)
    }
    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip', 'r'
            ).open(f'{archive_id:08n}.csv')
        else:
            # =============================================================================
            # kwargs['filepath_or_buffer'] = ZipFile(io.BytesIO(
            #     requests.get(url).content)
            # ).open(f'{archive_id:08n}.csv')
            # =============================================================================
            pass
    return pd.read_csv(**kwargs)


def stockpile_can(series_ids: dict[str, int]) -> DataFrame:
    """


    Parameters
    ----------
    series_ids : dict[str, int]
        DESCRIPTION.

    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================

    """
    return pd.concat(
        [
            read_can(archive_id).pipe(
                pull_by_series_id, series_id
            )
            for series_id, archive_id in series_ids.items()
        ],
        axis=1,
        verify_integrity=True,
        sort=True
    )


def shadow_append(df, series_ids):
    data = df.loc[:, series_ids].dropna(how="all")
    data["_".join((*series_ids, "sum"))] = data.sum(axis=1)
    return data


os.chdir(path_src)

result = DataFrame()
combined = DataFrame()
FILE_NAME = 'stat_can_prd.csv'
data = read_temporary(FILE_NAME)


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


FILE_NAME = 'stat_can_lab.csv'
data = read_temporary(FILE_NAME)
SERIES_IDS = {
    'v74989': 14100235,
    'v2057609': 14100355,
    'v123355112': 14100355,
    'v2057818': 14100355,
}
combined = data.loc[:, SERIES_IDS].dropna(how="all")
combined = combined.div(combined.loc[1982]).mul(100)
combined['mean'] = combined.mean(axis=1)
result = pd.concat([result, combined.iloc[:, [-1]]], axis=1)


combined = DataFrame()
FILE_NAME = 'stat_can_prd.csv'
data = read_temporary(FILE_NAME)


SERIES_IDS = {
    'v21573686': 36100207,  # Total number of jobs
    'v111382232': 36100480,  # Total number of jobs
}
combined = data.loc[:, SERIES_IDS].dropna(how="all")
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
FILE_NAME = 'stat_can_lab.csv'
data = read_temporary(FILE_NAME)


SERIES_IDS = {
    'v249139': 14100265,
    'v2523013': 14100027,
    'v1596771': 14100238,
    'v78931172': 14100243,
    'v65521825': 36100489
}
combined = data.loc[:, SERIES_IDS].dropna(how="all")


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


combined = DataFrame()
FILE_NAME = 'stat_can_lab.csv'
data = read_temporary(FILE_NAME)
SERIES_IDS = {
    'v1235071986': 14100392,
}
combined = data.loc[:, SERIES_IDS].dropna(how="all")
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
# .get_figure().savefig('view_canada.pdf', format='pdf', dpi=900)
# result.to_excel('result.xlsx')
#


FILE_NAME = 'series_ids.xlsx'
data = pd.read_excel(Path(path_src).joinpath(FILE_NAME))
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
data = pd.read_csv(Path(path_src).joinpath(FILE_NAME))
a = set(data.T.index)
a.remove('REF_DATE')
FILE_NAME = 'stat_can_lab.csv'
data = pd.read_csv(Path(path_src).joinpath(FILE_NAME))
b = set(data.T.index)
b.remove('REF_DATE')
FILE_NAME = 'stat_can_prd.csv'
data = pd.read_csv(Path(path_src).joinpath(FILE_NAME))
c = set(data.T.index)
c.remove('REF_DATE')


a -= refactd
b -= refactd
c -= refactd
