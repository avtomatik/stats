#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Feb 13 18:09:40 2022

@author: Alexander Mikhailov
"""


import zipfile
import ExcelFile
import pandas as pd
from core.config import BASE_DIR, DATA_DIR


def read_usa_bea_meta(xl_file: ExcelFile, sheet_name: str) -> pd.DataFrame:
    """
    Retrieves pd.DataFrame for Meta Information from Bureau of Economic Analysis Zip Archives

    Parameters
    ----------
    xl_file : ExcelFile
    sheet_name : str

    Returns
    -------
    pd.DataFrame
        ================== =================================
        df.index           Index
        df.iloc[:, 0]      Table
        df.iloc[:, 1]      Measure Unit
        df.iloc[:, 2]      Frequency Period
        df.iloc[:, 3]      Service
        df.iloc[:, 4]      Data Published
        df.iloc[:, 5]      File Created
        ================== =================================
    """
    kwargs = {
        'io': xl_file,
        'sheet_name': sheet_name,
        'header': None,
        'nrows': 6,
        'usecols': range(1)
    }
    return pd.read_excel(**kwargs).transpose()


def grab_usa_bea_archive_meta(df: pd.DataFrame, wb_name: str, sheet_name: str) -> pd.DataFrame:
    df.columns = (
        'table', 'unit', 'frequency_period', 'service', 'data_published', 'file_created'
    )
    df.iloc[:, -1] = df.iloc[:, -1].str.replace(
        'File created ', ''
    ).apply(pd.to_datetime)
    df['wb_name'] = wb_name
    df['sheet_name'] = sheet_name
    return df


def main(
    archive_name: str = 'dataset_usa_bea-release-2015-02-27-SectionAll_xls_1969_2015.zip',
    file_name: str = 'usa_bea_release-2015-02-27_meta.xlsx'
) -> None:
    with zipfile.ZipFile(DATA_DIR.joinpath(archive_name)) as archive:
        df = pd.DataFrame()
        for wb_name in archive.namelist():
            print('{:=^50}'.format('New File'))
            with pd.ExcelFile(archive.open(wb_name)) as xl_file:
                df = pd.concat(
                    [
                        df,
                        pd.concat(
                            map(
                                lambda _: read_usa_bea_meta(xl_file, _).pipe(
                                    grab_usa_bea_archive_meta, wb_name, _
                                ),
                                xl_file.sheet_names[1:]
                            )
                        )
                    ]
                )
    df.to_csv(BASE_DIR.joinpath(file_name), index=False)


if __name__ == '__main__':
    main()
