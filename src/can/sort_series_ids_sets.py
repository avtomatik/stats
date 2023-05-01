from pathlib import Path

import pandas as pd


def sort_series_ids_sets(path_src):
    FILE_NAME = 'series_ids.xlsx'
    data = pd.read_excel(
        Path(path_src).joinpath(FILE_NAME)
    ).dropna(axis=0, how='all').dropna(axis=1, how='all').fillna('None')
# =============================================================================
# data.to_excel(Path(path_export).joinpath(FILE_NAME), index=False)
# =============================================================================

    version = sorted(data.iloc[:, 0].unique())[0]
    chunk = data[data.iloc[:, 0] == version].iloc[:, 1:]
    chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
    SERIES_IDS_INIT = set(chunk.iloc[:, 0])
    version = sorted(data.iloc[:, 0].unique())[2]
    chunk = data[data.iloc[:, 0] == version].iloc[:, 1:]
    chunk = chunk[chunk.iloc[:, 0] == "# Labor"].iloc[:, 1:]
    SERIES_IDS_FINAL = set(chunk.iloc[:, 0])
# =============================================================================
# for series_id in sorted(SERIES_IDS_INIT and SERIES_IDS_FINAL):
#     print(series_id)
# =============================================================================

# =============================================================================
# Test Series IDS
# =============================================================================

    def get_columns_set(file_name: str, path_src: str) -> set[str]:
        kwargs = {
            'filepath_or_buffer': Path(path_src).joinpath(file_name),
            'index_col': 0
        }
        return set(pd.read_csv(**kwargs).columns)

    FILE_NAME = 'stat_can_cap.csv'
    SERIES_IDS_CAP = get_columns_set(FILE_NAME, path_src)

    FILE_NAME = 'stat_can_lab.csv'
    SERIES_IDS_LAB = get_columns_set(FILE_NAME, path_src)

    FILE_NAME = 'stat_can_prd.csv'
    SERIES_IDS_PRD = get_columns_set(FILE_NAME, path_src)

    SERIES_IDS_CAP -= SERIES_IDS_FINAL
    SERIES_IDS_LAB -= SERIES_IDS_FINAL
    SERIES_IDS_PRD -= SERIES_IDS_FINAL
