#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Mar 26 16:01:08 2023

@author: green-machine
"""


import zipfile
from functools import cache
from pathlib import Path

import pandas as pd

from .constants import MAP_READ_CAN


@cache
def read_can(archive_id: int) -> pd.DataFrame:
    """


    Parameters
    ----------
    archive_id : int

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 10, 12]))
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            # =============================================================================
            # kwargs['filepath_or_buffer'] = zipfile.ZipFile(io.BytesIO(
            #     requests.get(url).content)
            # ).open(f'{archive_id:08n}.csv')
            # =============================================================================
            pass
    return pd.read_csv(**kwargs)


def read_can_groupby(file_id: int) -> pd.DataFrame:
    """


    Parameters
    ----------
    file_id : int
        DESCRIPTION.

    Returns
    -------
    pd.DataFrame
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
def read_can_sandbox(archive_id: int) -> pd.DataFrame:
    """
    Parameters
    ----------
    archive_id : int
    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Period
        ...                ...
        df.iloc[:, -1]     Values
        ================== =================================
    """
    MAP_DEFAULT = dict(zip(['period', 'series_id', 'value'], [0, 10, 12]))
    MAP_ARCHIVE_ID_FIELD = {
        310004: dict(zip(['period', 'prices', 'category', 'component', 'series_id', 'value'], [0, 2, 4, 5, 6, 8])),
        2820011: dict(zip(['period', 'geo', 'classofworker', 'industry', 'sex', 'series_id', 'value'], [0, 1, 2, 3, 4, 5, 7])),
        2820012: dict(zip(['period', 'series_id', 'value'], [0, 5, 7])),
        3790031: dict(zip(['period', 'geo', 'seas', 'prices', 'naics', 'series_id', 'value'], [0, 1, 2, 3, 4, 5, 7])),
        3800084: dict(zip(['period', 'geo', 'seas', 'est', 'series_id', 'value'], [0, 1, 2, 3, 4, 6])),
        3800102: dict(zip(['period', 'series_id', 'value'], [0, 4, 6])),
        3800106: dict(zip(['period', 'series_id', 'value'], [0, 3, 5])),
        3800518: dict(zip(['period', 'series_id', 'value'], [0, 4, 6])),
        3800566: dict(zip(['period', 'series_id', 'value'], [0, 3, 5])),
        3800567: dict(zip(['period', 'series_id', 'value'], [0, 4, 6])),
        36100096: dict(
            zip(
                [
                    'period',
                    # =============================================================================
                    #                 'geo', 'prices', 'industry', 'category', 'component',
                    # =============================================================================
                    'series_id', 'value'
                ],
                [
                    0,
                    # =============================================================================
                    #                 1, 3, 4, 5, 6,
                    # =============================================================================
                    11, 13
                ]
            )
        ),
        36100303: dict(zip(['period', 'series_id', 'value'], [0, 9, 11])),
        36100305: dict(zip(['period', 'series_id', 'value'], [0, 9, 11])),
        36100236: dict(zip(['period', 'series_id', 'value'], [0, 11, 13]))
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
            kwargs['filepath_or_buffer'] = zipfile.ZipFile(
                f'{archive_id:08n}-eng.zip'
            ).open(f'{archive_id:08n}.csv')
        else:
            # =============================================================================
            # kwargs['filepath_or_buffer'] = zipfile.ZipFile(io.BytesIO(
            #     requests.get(url).content)
            # ).open(f'{archive_id:08n}.csv')
            # =============================================================================
            pass
    return pd.read_csv(**kwargs)
