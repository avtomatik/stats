#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 16:01:08 2023

@author: green-machine
"""


from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from pandas import DataFrame

from .constants import MAP_READ_CAN


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
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
    kwargs = {
        'header': 0,
        'names': list(MAP_READ_CAN.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_READ_CAN.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }
    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            # =============================================================================
            # kwargs['filepath_or_buffer'] = ZipFile(io.BytesIO(
            #     requests.get(url).content)
            # ).open(f'{archive_id:08n}.csv')
            # =============================================================================
            pass
    return pd.read_csv(**kwargs)


def read_can_groupby(file_id: int) -> DataFrame:
    """


    Parameters
    ----------
    file_id : int
        DESCRIPTION.

    Returns
    -------
    DataFrame
        DESCRIPTION.

    """
    FILE_IDS = (5245628780870031920, 7931814471809016759, 8448814858763853126)
    SKIPROWS = (3, 241, 81)
    kwargs = {
        'filepath_or_buffer': f'dataset_can_cansim{file_id:n}.csv',
        'index_col': 0,
        'skiprows': dict(zip(FILE_IDS, SKIPROWS)).get(file_id),
        'parse_dates': file_id == 5245628780870031920
    }

    df = pd.read_csv(**kwargs)
    if file_id == 7931814471809016759:
        df.columns = map(int, map(lambda _: _[:7].split()[-1], df.columns))
        df.iloc[:, -1] = df.iloc[:, -1].str.replace(
            ';', ''
        ).apply(pd.to_numeric)
        df = df.transpose()
    if file_id == 5245628780870031920:
        return df.groupby(df.index.year).mean().rename_axis('period')
    return df.groupby(df.index).mean().rename_axis('period')


@cache
def read_can_sandbox(archive_id: int) -> DataFrame:
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
    MAP_ARCHIVE_ID_FIELD = {
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
            # =============================================================================
            #             'geo': 1,
            #             'prices': 3,
            #             'industry': 4,
            #             'category': 5,
            #             'component': 6,
            # =============================================================================
            'series_id': 11,
            'value': 13
        },
        36100303: {'period': 0, 'series_id': 9, 'value': 11},
        36100305: {'period': 0, 'series_id': 9, 'value': 11},
        36100236: {'period': 0, 'series_id': 11, 'value': 13}
    }
    url = f'https://www150.statcan.gc.ca/n1/tbl/csv/{archive_id:08n}-eng.zip'
    TO_PARSE_DATES = (
        2820011, 3790031, 3800084, 10100094, 14100221, 14100235, 14100238, 14100355, 16100109, 16100111, 36100108, 36100207, 36100434
    )
    kwargs = {
        'header': 0,
        'names': list(MAP_ARCHIVE_ID_FIELD.get(archive_id, MAP_DEFAULT).keys()),
        'index_col': 0,
        'usecols': list(MAP_ARCHIVE_ID_FIELD.get(archive_id, MAP_DEFAULT).values()),
        'parse_dates': archive_id in TO_PARSE_DATES
    }
    if archive_id < 10 ** 7:
        kwargs['filepath_or_buffer'] = f'{archive_id:08n}-eng.zip'
    else:
        if Path(f'{archive_id:08n}-eng.zip').is_file():
            kwargs['filepath_or_buffer'] = ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            # =============================================================================
            # kwargs['filepath_or_buffer'] = ZipFile(io.BytesIO(
            #     requests.get(url).content)
            # ).open(f'{archive_id:08n}.csv')
            # =============================================================================
            pass
    return pd.read_csv(**kwargs)


# =============================================================================
# Not Clear
# =============================================================================
def read_can_special() -> DataFrame:
    kwargs = {
        'filepath_or_buffer': 'dataset_read_can-{:08n}-eng-{}.csv'.format(
            310003, 7591839622055840674
        ),
        'skiprows': 3,
    }
    return pd.read_csv(**kwargs)
