#!/usr/bin/python3
from sys import exit
from math import sqrt
import numpy as np
from numpy import array
from collections import namedtuple
from scipy.optimize import fmin_l_bfgs_b
import pandas as pd
from itertools import chain
#import matplotlib.pylab as plt
from PyAstronomy import pyasl
#def running_median_insort
from collections import deque
from bisect import insort, bisect_left
from itertools import islice
from os.path import basename
import  pickle
from datetime import datetime, timedelta
#import my defined functions
import reader
'''
prepare_mobile.py
终端执行：curl -XPUT http://127.0.0.1:9200/mobile-mbank-log-2018.01/_settings -d '{ "index" : { "max_result_window" : 100000000}}'
'''
def clean_mean(df,fields,starttime,endtime):   #the output of the  function is every minute mean value
    df=df.filter(items=fields)
    df.columns=["check_time","value"]
    df=df.sort_values(by="check_time")

#checktime have error numbers
    df=df[(df["check_time"]>starttime) & (df["check_time"]<endtime)]
#every minute
    df['minute']=df['check_time'].str[0:16]+":00"
#-----usetime
    temp=df[np.isnan(df['value'])==False]
    cleandata=round(temp.groupby(temp['minute'])['value'].mean(),2)
    cleandata=pd.DataFrame(cleandata)
    cleandata['time']=cleandata.index
    

#-----whole minute list--
    starttime =pd.to_datetime(min(df['minute']),format='%Y-%m-%d %H:%M:%S')
    endtime   =pd.to_datetime(max(df['minute']),format='%Y-%m-%d %H:%M:%S')
    timelist=[starttime.strftime("%Y-%m-%d %H:%M:%S")]
    while endtime >starttime:
        starttime += timedelta(seconds=60)
        timelist.append(starttime.strftime("%Y-%m-%d %H:%M:%S"))
    timelist=pd.DataFrame(timelist)
    timelist.columns=["time"]
    #print (type(cleandata))
    #print (cleandata.head(10))
#---------------merge the data----
    data=pd.merge(timelist,cleandata)   
#---------------save/return  the result data for the model
    data.columns=["datetime","value"]  #string,num
    #data["time"]=pd.to_datetime(data["time"])
    return data
#cleandata.to_csv("/home/voyager/data/mobile/UseTime")

# output==>model :[time,value]
#persitence:"/home/voyager/data/tploader/transcount"
def running_median_insort(seq, window_size): #中位数
    """Contributed by Peter Otten"""
    seq = iter(seq)
    d = deque()
    s = []
    result = []
    for item in islice(seq, window_size):
        d.append(item)
        insort(s, item)
        result.append(s[len(d)//2])
    m = window_size // 2
    for item in seq:
        old = d.popleft()
        d.append(item)
        del s[bisect_left(s, old)]
        insort(s, item)
        result.append(s[m])
    return result 


