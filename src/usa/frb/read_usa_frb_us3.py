#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 21:42:17 2022

@author: Alexander Mikhailov
"""

import io
from zipfile import ZipFile

import requests
from lxml import etree
from pandas import DataFrame

URL = "https://www.federalreserve.gov/datadownload/Output.aspx?rel=g17&filetype=zip"

with ZipFile(io.BytesIO(requests.get(URL).content)) as archive:
    # =========================================================================
    # Select the Largest File Containing the Most of the Data
    # =========================================================================
    _map = {_.file_size: _.filename for _ in archive.filelist}
    with archive.open(_map[max(_map)]) as f:
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
        frdf = DataFrame(rows, columns=cols)
