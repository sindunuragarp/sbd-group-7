#!/usr/bin/env python3
# pylint: disable=C0103,C0111
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  8 17:43:54 2017

@author: sindunuragarp
"""

import os
import pandas as pd
from fastparquet import ParquetFile

###############################################################################

directory = "../data/output/"
files = [directory + f for f in os.listdir(directory) if f.endswith('.parquet')]

frames = []
for file in files:
    pf = ParquetFile(file)
    df = pf.to_pandas()
    frames.append(df)
    
data = pd.concat(frames)

###############################################################################

print(data)
