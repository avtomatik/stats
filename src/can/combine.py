import pandas as pd
from pandas import DataFrame

from stats.src.can.stockpile import stockpile_can
from thesis.src.lib.transform import (transform_deflator, transform_pct_change,
                                      transform_sum)


def combine_can_price_a(series_ids: tuple[dict[str, int]]) -> DataFrame:
    return pd.concat(
        map(lambda _: stockpile_can(_).pipe(transform_deflator), series_ids),
        axis=1,
        sort=True
    )


def combine_can_price_b(series_ids: tuple[dict[str, int]]) -> DataFrame:
    return pd.concat(
        map(lambda _: stockpile_can(_).pipe(transform_pct_change), series_ids),
        axis=1
    )


def combine_can_plain_or_sum(series_ids: dict[str, int]) -> DataFrame:
    if len(series_ids) > 1:
        return stockpile_can(series_ids).pipe(
            transform_sum,
            name='_'.join((*series_ids, 'sum'))
        )
    return stockpile_can(series_ids)
