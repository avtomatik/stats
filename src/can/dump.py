from functools import cache
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from pandas import DataFrame

from thesis.src.lib.collect import stockpile_can
from thesis.src.lib.read import read_can
from thesis.src.lib.tools import archive_name_to_url
from thesis.src.lib.transform import (transform_agg_sum, transform_deflator,
                                      transform_pct_change)


def read_can_group_a(file_id: int, **kwargs) -> DataFrame:
    """
    Parameters
    ----------
    file_id : int
        DESCRIPTION.
    **kwargs : TYPE
        DESCRIPTION.
    Returns
    -------
    DataFrame
        DESCRIPTION.
    """
    # =========================================================================
    # Not Used Anywhere
    # =========================================================================
    kwargs['filepath_or_buffer'] = f'dataset_read_can{file_id:n}.csv'
    kwargs['index_col'] = 0
    df = pd.read_csv(**kwargs)
    if file_id == 7931814471809016759:
        df.columns = (column[:7] for column in df.columns)
        df.iloc[:, -1] = pd.to_numeric(df.iloc[:, -1].str.replace(';', ''))
    df = df.transpose()
    df['period'] = pd.to_numeric(
        df.index.astype(str).to_series().str.slice(start=3),
        downcast='integer'
    )
    return df.groupby(df.columns[-1]).mean()


def read_can_group_b(file_id: int, **kwargs) -> DataFrame:
    """
    Parameters
    ----------
    file_id : int
        DESCRIPTION.
    **kwargs : TYPE
        DESCRIPTION.
    Returns
    -------
    DataFrame
        DESCRIPTION.
    """
    # =========================================================================
    # Not Used Anywhere
    # =========================================================================
    kwargs['filepath_or_buffer'] = f'dataset_read_can{file_id:n}.csv'
    kwargs['index_col'] = 0
    df = pd.read_csv(**kwargs)
    df['period'] = pd.to_numeric(
        df.index.astype(str).to_series().str.slice(start=4),
        downcast='integer'
    )
    return df.groupby(df.columns[-1]).mean()


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


def build_push_data_frame(path_or_buf: str, blueprint: dict) -> None:
    """
    Builds DataFrame & Loads It To CSV
    Parameters
    ----------
    path_or_buf : str
        Excel File Name.
    blueprint : dict
        DESCRIPTION.
    Returns
    -------
    None
    """
    df = DataFrame()
    for entry in blueprint:
        _df = read_can(archive_name_to_url(entry['archive_name']))
        _df = _df[_df['VECTOR'].isin(entry['series_ids'])]
        for series_id in entry['series_ids']:
            chunk = _df[_df['VECTOR'] == series_id][['VALUE']]
            chunk = chunk.groupby(chunk.index.year).mean()
            df = pd.concat([df, chunk], axis=1, sort=True)
        df.columns = entry['series_ids']
    df.to_csv(path_or_buf)


read_can_group_a(7931814471809016759, skiprows=241)
read_can_group_a(8448814858763853126, skiprows=81)
read_can_group_b(5245628780870031920, skiprows=3)
stockpile_can({'v62143969': 36100108}).pipe(transform_agg_sum)
stockpile_can({'v62143990': 36100108}).pipe(transform_agg_sum)