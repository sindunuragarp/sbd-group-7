#!/usr/bin/env python3
# pylint: disable=C0103,C0111
# -*- coding: utf-8 -*-
"""
Created on Sun Oct  8 17:43:54 2017

@author: sindunuragarp
"""

import argparse
import os
import pandas as pd
from fastparquet import ParquetFile

###############################################################################

parser = argparse.ArgumentParser("Read a folder of parquet files")
parser.add_argument("--input", '-i', metavar="segment_index",
                    type=str, required=True,
                    help="path to directory")
                    
conf = parser.parse_args()

###############################################################################

directory = conf.input
if directory is None:
    directory = "data/output/"

frames = []
files = [directory + f for f in os.listdir(directory) if f.endswith('.parquet')]
for file in files:
    pf = ParquetFile(file)
    df = pf.to_pandas()
    frames.append(df)
    
data = pd.concat(frames)

###############################################################################

print(data)
