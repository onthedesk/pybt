

import pybt

import pandas as pd

import numpy as np

import tushare as ts

# get data

def test_get_h_data():

    data = ts.get_h_data('600848', start='2016-09-02', end='2016-09-21')

    assert data


