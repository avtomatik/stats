from pandas import DataFrame


def transform_year_mean(df: DataFrame) -> DataFrame:
    return df.groupby(df.index.year).mean()