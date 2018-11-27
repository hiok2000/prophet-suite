#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""Frequency estimation via periodograms
   https://github.com/welch/seasonal/blob/master/seasonal/periodogram.py

    python3 seasonality.py --es_host 192.168.0.21 --es_port 9900  --input_name core-tploader-*   --reader_module reader  --reader_function average_by_ts  --field  duration --start 2018-03-08T00:00:00  --end 2018-05-08T00:00:00

python3 seasonality.py --es_host 98.11.56.20 --es_port 10092  --input_name core-tploader-*   --reader_module reader  --reader_function average_by_ts  --field  duration --start 2018-03-08T00:00:00  --end 2018-05-08T00:00:00	

"""
from __future__ import division
from sys import exit
from math import sqrt
import numpy as np
from numpy import array
from collections import namedtuple
from scipy.optimize import fmin_l_bfgs_b
import pandas as pd
from itertools import chain
from PyAstronomy import pyasl
from collections import deque
from bisect import insort, bisect_left
from itertools import islice
from os.path import basename
import  pickle
import math
import datetime
import time
import json
from elasticsearch import Elasticsearch

# framework
import sys
sys.path.append('..')
from Operator import Operator

# local dependencies
sys.path.append('../unsupervised_learning/lib')
import reader
import clean
import dshw

import numpy as np
import scipy.signal

# by default, assume at least this many cycles of data when
# establishing FFT window sizes.
MIN_FFT_CYCLES = 3.0    #最少有3个周期

# by default, assume periods of no more than this when establishing
# FFT window sizes.
MAX_FFT_PERIOD = 100000    #lz：512。最长周期不超过

def periodogram_peaks(data, min_period=4, max_period=None, thresh=0.90):
    """return a list of intervals containg high-scoring periods
    Use a robust periodogram to estimate ranges containing
    high-scoring periodicities in possibly short, noisy data. Returns
    each peak period along with its adjacent bracketing periods from
    the FFT coefficient sequence.
    Data should be detrended for sharpest results, but trended data
    can be accommodated by lowering thresh (resulting in more
    intervals being returned)
    Parameters
    ----------
    data : ndarray
        Data series, evenly spaced samples.  #fields :index ,value
    min_period : int
        Disregard periods shorter than this number of samples.
        Defaults to 4
    max_period : int
        Disregard periods longer than this number of samples.
        Defaults to the smaller of len(data)/MIN_FFT_CYCLES or MAX_FFT_PERIOD
    thresh : float (0..1)
        Retain periods scoring above thresh*maxscore. Defaults to 0.9
    Returns
    -------
    periods : array of quads, or None
        Array of (period, power, period-, period+), maximizing period
        and its score, and FFT periods bracketing the maximizing
        period, returned in decreasing order of score
    """
    periods, power = periodogram(data, min_period, max_period)
    test = pd.DataFrame(periods,columns=["periods[records]"])
    test["power"] = power
    test=test.sort_values("power",ascending=False)
    print ("-----------------------the most likely seasonality---------------------------")
    print (test.head(5))   #四舍五入方法
    try :
        if np.all(np.isclose(power, 0.0)):
            print ("There is not seasonality , the recommended  model  is 'simple-normal-prophet.py' 。")
            return None # DC
        elif abs(test.iloc[0,0]-1440)<720 and abs(test.iloc[1,0]-1440*7)>720 :
            print ("There is 1 seasonality: 1 day , the recommended  model  is 'single-normal-prophet.py' 。") 

        elif (abs(test.iloc[0,0]-1440)<720 or abs(test.iloc[1,0]-1440)<720 ) and (abs(test.iloc[0,0]-1440*7)<720 or abs(test.iloc[1,0]-1440*7)<720) :
            print ("There are 2 seasonality: 1 day and  1 week, the recommended  model  is 'double-dshw-prophet.py' 。") 
        else :
             print ("There is no recommended models .According to the table of periods , you can make your own choices。")

    except (Exception):
        print (Exception)

    result = []
    keep = power.max() * thresh
    while True:
        peak_i = power.argmax()
        if power[peak_i] < keep:
            break
        min_period = periods[min(peak_i + 1, len(periods) - 1)]
        max_period = periods[max(peak_i - 1, 0)]
        result.append([periods[peak_i], power[peak_i], min_period, max_period])
        power[peak_i] = 0
    return result if len(result) else None

def periodogram(data, min_period=4, max_period=None):
    """score periodicities by their spectral power.
    Produce a robust periodogram estimate for each possible periodicity
    of the (possibly noisy) data.
    Parameters
    ----------
    data : ndarray
        Data series, having at least three periods of data.
    min_period : int
        Disregard periods shorter than this number of samples.
        Defaults to 4
    max_period : int
        Disregard periods longer than this number of samples.
        Defaults to the smaller of len(data)/MIN_FFT_CYCLES or MAX_FFT_PERIOD
    Returns
    -------
    periods, power : ndarray, ndarray
        Periods is an array of Fourier periods in descending order,
        beginning with the first one greater than max_period.
        Power is an array of spectral power values for the periods
    Notes
    -----
    This uses Welch's method (no relation) of periodogram
    averaging[1]_, which trades off frequency precision for better
    noise resistance. We don't look for sharp period estimates from
    it, as it uses the FFT, which evaluates at periods N, N/2, N/3, ...,
    so that longer periods are sparsely sampled.
    References
    ----------
    .. [1]: https://en.wikipedia.org/wiki/Welch%27s_method
    """
    if max_period is None:
        max_period = int(min(len(data) / MIN_FFT_CYCLES, MAX_FFT_PERIOD))
    nperseg = min(max_period * 2, len(data) // 2) # FFT window
    freqs, power = scipy.signal.welch(
        data, 1.0, scaling='spectrum', nperseg=nperseg)
    periods = np.array([int(round(1.0 / freq)) for freq in freqs[1:]])
    power = power[1:]
    # take the max among frequencies having the same integer part
    idx = 1
    while idx < len(periods):
        if periods[idx] == periods[idx - 1]:
            power[idx-1] = max(power[idx-1], power[idx])
            periods, power = np.delete(periods, idx), np.delete(power, idx)
        else:
            idx += 1
    power[periods == nperseg] = 0 # disregard the artifact at nperseg
    min_i = len(periods[periods >= max_period]) - 1
    max_i = len(periods[periods < min_period])
    periods, power = periods[min_i : -max_i], power[min_i : -max_i]
    return periods, power


class estimate_season(Operator):
    def __init__(self):
        super(estimate_season, self).__init__()
        self.parse_args()

    def parse_args(self):
        # add my own args
        self._parser.add_argument('--start', type=str,
            default="2018-03-08T00:00:00",
            help='the start datetime')

        self._parser.add_argument('--end', type=str,
            default="2018-12-08T00:00:00",
            help='the end datetime')



        # always call super at the end of parse_args
        super(estimate_season, self).parse_args()

    def read_time_range_data(self):
        self.df = self.call(
            self._flags.reader_module,
            self._flags.reader_function,
            [],
            {
                'es_host': self._flags.es_host,    #Operator
                'es_port': self._flags.es_port,    #Operator
                'data_index': self._flags.input_name,  #Operator
                'field': self._flags.field,            #Operator
                'start': self._flags.start,            #
                'end': self._flags.end
            }
        )

    def main(self):
            self.read_time_range_data()
            print ("-----------------------the source data---------------------------")
            print (self.df.head(5))
            s=self.df["cleanvalue"]
            p = periodogram_peaks(s, thresh=1.0)
            

if __name__=="__main__":
    period = estimate_season()
    period.main()
