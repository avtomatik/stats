from pathlib import Path

import pandas as pd

from stats.src.common.funcs import get_pre_kwargs


def sort_series_ids_sets(path_src: str):
    FILE_NAME = 'series_ids.xlsx'
    df = pd.read_excel(
        Path(path_src).joinpath(FILE_NAME)
    ).dropna(axis=0, how='all').dropna(axis=1, how='all').fillna('None')
# =============================================================================
# df.to_csv(Path(path_export).joinpath(FILE_NAME), index=False)
# =============================================================================

    version = sorted(df.iloc[:, 0].unique())[0]
    chunk = df[df.iloc[:, 0] == version].iloc[:, 1:]
    chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
    SERIES_IDS_INIT = set(chunk.iloc[:, 0])
    version = sorted(df.iloc[:, 0].unique())[2]
    chunk = df[df.iloc[:, 0] == version].iloc[:, 1:]
    chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
    SERIES_IDS_FINAL = set(chunk.iloc[:, 0])
# =============================================================================
# for series_id in sorted(SERIES_IDS_INIT and SERIES_IDS_FINAL):
#     print(series_id)
# =============================================================================

# =============================================================================
# Test Series IDS Consistency
# =============================================================================

    FILE_NAME = 'stat_can_cap.csv'
    SERIES_IDS_CAP = set(pd.read_csv(**get_pre_kwargs(FILE_NAME)).columns)

    FILE_NAME = 'stat_can_lab.csv'
    SERIES_IDS_LAB = set(pd.read_csv(**get_pre_kwargs(FILE_NAME)).columns)

    FILE_NAME = 'stat_can_prd.csv'
    SERIES_IDS_PRD = set(pd.read_csv(**get_pre_kwargs(FILE_NAME)).columns)

    SERIES_IDS_CAP -= SERIES_IDS_FINAL
    SERIES_IDS_LAB -= SERIES_IDS_FINAL
    SERIES_IDS_PRD -= SERIES_IDS_FINAL
