# -*- coding: utf-8 -*-
"""
Created on Tue Apr 28 00:37:30 2020

@author: Alexander Mikhailov
"""


import pandas as pd
from core.config import DATA_DIR
from stats.src.common.funcs import get_file_names, get_xl_sheetnames


def main():

    for file_name in get_file_names(DATA_DIR):

        for sheet_name in get_xl_sheetnames(DATA_DIR.joinpath(file_name)):
            print(pd.read_excel(DATA_DIR.joinpath(file_name), sheet_name))


if __name__ == '__main__':
    main()
