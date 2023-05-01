import pandas as pd

from stats.src.can.stockpile import stockpile_can
from thesis.src.lib.transform import transform_deflator, transform_pct_change


def combine_can_price_a(series_ids: tuple[dict[str, int]]) -> pd.DataFrame:
    return pd.concat(
        map(lambda _: stockpile_can(_).pipe(transform_deflator), series_ids),
        axis=1,
        sort=True
    )


def combine_can_price_b(series_ids: tuple[dict[str, int]]) -> pd.DataFrame:
    return pd.concat(
        map(lambda _: stockpile_can(_).pipe(transform_pct_change), series_ids),
        axis=1
    )
