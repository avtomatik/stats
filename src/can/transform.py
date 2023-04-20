from pandas import DataFrame


def mean_series_id(df: DataFrame) -> DataFrame:
    return df.groupby(df.index.year).mean()


def transform_sum(df: DataFrame, name: str) -> DataFrame:
    """
    Parameters
    ----------
    df : DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, ...]    Series
        ================== =================================
    name : str
        New Column Name.
    Returns
    -------
    DataFrame
        ================== =================================
        df.index           Period
        df.iloc[:, 0]      Sum of <series_ids>
        ================== =================================
    """
    df[name] = df.sum(axis=1)
    return df.iloc[:, [-1]]
