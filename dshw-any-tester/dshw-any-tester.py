from sys import exit
from math import sqrt
import numpy as np
from numpy import array
from collections import namedtuple
from scipy.optimize import fmin_l_bfgs_b
import pandas as pd
from itertools import chain
import matplotlib.pylab as plt
from PyAstronomy import pyasl
from collections import deque
from bisect import insort, bisect_left
from itertools import islice
import pickle
from elasticsearch import Elasticsearch

import sys
sys.path.append('.')
from Operator import Operator

class DSHW_Tester(Operator):
    def __init__(self):
        super(DSHW_Tester, self).__init__()

        self.parse_args()
        self.load_model()

    def parse_args(self):
        self._parser.add_argument('--model_path', type=str,
            default='/opt/voyager/prophet-suite/data/',
            help='Model export path')

        self._parser.add_argument('--data_index', type=str,
            default='transcount',
            help='Data index')

        super(DSHW_Tester, self).parse_args()

    def load_model(self):
        with open(self._flags.model_path + self._flags.data_index + '_params', 'rb') as in_pickle:
            myparams = pickle.load(in_pickle)

        [self._flags.data_index, self._params, self._st] = myparams
        self._df = pd.read_pickle(self._flags.model_path + self._flags.data_index + '_df')
        self._outliners1 = pd.read_pickle(self._flags.model_path + self._flags.data_index + "_outliers")

    def predict(self, input):
        time = input[0]
        value = input[1]

        estimate = self.df.loc[df["TIME"] == time, "estimate"].iloc[0]
        residual = value - estimate
        upper_bound = round(estimate + 4 * self._st)
        lower_bound = round(estimate - 4 * self._st)
        anom_index = round(residual / self._st, 1)
        GESD_TEST = np.where(anom_index > outliers1.loc[outliers1["anom_index"] == 1, "anom_index"].min(), 1, 0)
        BOUND_TEST = np.where(anom_index > outliers1["anom_index"].min(), 1, 0)


if __name__=="__main__":
    file="transcount"
    with open("/home/voyager/project/data/"+file+"_params", 'rb') as in_pickle:
        myparams = pickle.load(in_pickle) ## ["a", "ab", "ac"]

    file=myparams[0]
    params=myparams[1]
    st=myparams[2]

    df=pd.read_pickle("/home/voyager/project/data/"+file+"_df")
    outliers1=pd.read_pickle("/home/voyager/project/data/"+file+"_outliers")


    newline=["2017-03-01 00:05:00",2234]
    TIME=newline[0]
    value=newline[1]
   
    estimate=df.loc[df["TIME"]==TIME,"estimate"].iloc[0]
    residual=value-estimate
    upper_bound=round(estimate+4*st)
    lower_bound=round(estimate-4*st)
    anom_index=round(residual/st,1)
    GESD_TEST=np.where(anom_index>outliers1.loc[outliers1["anom_index"]==1,"anom_index"].min(),1,0)
    BOUND_TEST=np.where(anom_index>outliers1["anom_index"].min(),1,0)

    line=[(TIME,value,estimate,residual,upper_bound,lower_bound,anom_index,GESD_TEST,BOUND_TEST)]
    labels=["TIME","value","estimate","residual","upper_bound","lower_bound","anom_index","GESD_TEST","BOUND_TEST"]
   
    result=pd.DataFrame.from_records(line,columns=labels)

    if GESD_TEST==1:
        #outliers1=outliers1.ix[:-1]
        outliers1=outliers1.append(result)
        outliers1=outliers1.sort_values(by="anom_index",ascending=False)
    print (result)