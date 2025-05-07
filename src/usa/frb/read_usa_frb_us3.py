#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 21:42:17 2022

@author: Alexander Mikhailov
"""

import io
import zipfile

import pandas as pd
import requests
from lxml import etree

URL = 'https://www.federalreserve.gov/datadownload/Output.aspx?rel=g17&filetype.zip'

with zipfile.ZipFile(io.BytesIO(requests.get(URL).content)) as archive:
    # =========================================================================
    # Select the Largest File with min() Function
    # =========================================================================
    with archive.open(
        min({_.filename: _.file_size for _ in archive.filelist})
    ) as f:
        tree = etree.parse(f)
        doc = tree.getroot()

        # your expected columns:
        cols = ["TIME_PERIOD", "IP.B50001.S"]

        # the base xpath expression
        expr = '//*[local-name()="Obs"]'
        rows = []
        for r in doc.xpath(expr):
            row = []

            # use more xpath expressions to get to the target attributes
            row.extend([r.xpath('.//@TIME_PERIOD')[0],
                        r.xpath('.//@OBS_VALUE')[0]])
            rows.append(row)
        frdf = pd.DataFrame(rows, columns=cols)
