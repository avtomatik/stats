from pathlib import Path

import pandas as pd
from pandas import DataFrame


def read_frb_gvp_sa() -> DataFrame:
    """'T50030: Final products and nonindustrial supplies--gross value'"""
    PATH = '/media/green-machine/KINGSTON'
    kwargs = {
        'filepath_or_buffer': Path(PATH).joinpath('dataset_usa_frb_gvp_sa.txt'),
        'sep': '\s+',
        'header': None,
        'skiprows': 1,
        'nrows': 29,
        'index_col': 1
    }
    return pd.read_csv(**kwargs)


if __name__ == '__main__':
    print(read_frb_gvp_sa())
