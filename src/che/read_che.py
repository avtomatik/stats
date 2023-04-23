# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 00:37:30 2020

@author: Alexander Mikhailov
"""

from pathlib import Path

import pandas as pd

from stats.src.common.utils import get_file_names, get_xl_sheetnames


def main(path_src: str = '/media/green-machine/KINGSTON'):

    for file_name in get_file_names(path_src):
        print(file_name)
        for sheet_name in get_xl_sheetnames(Path(path_src).joinpath(file_name)):
            print(pd.read_excel(Path(path_src).joinpath(file_name), sheet_name))


if __name__ == '__main__':
    main()
