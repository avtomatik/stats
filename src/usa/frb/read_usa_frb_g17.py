from zipfile import ZipFile

import pandas as pd

pd.options.display.max_columns = 8


with ZipFile('FRB_g17.zip') as archive:
    _map = {_.file_size: _.filename for _ in archive.filelist}
    # =====================================================================
    # Select the Largest File
    # =====================================================================
    with archive.open(_map[max(_map)]) as f:
        df = pd.read_xml(
            f,
            xpath=".//frb:DataSet",
            namespaces={
                "kf": "http://www.federalreserve.gov/structure/compact/G17_IP_MAJOR_INDUSTRY_GROUPS"}
        )
        df.to_excel('test.xlsx', index=False)
        df = pd.read_xml(
            f,
            index_col=0,
            skiprows=4
        ).dropna(axis=1, how='all').transpose()
        df.drop(df.index[:3], inplace=True)
        return df.rename_axis('period')
