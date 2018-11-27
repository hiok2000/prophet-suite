#!/usr/bin/python3

# -*- coding: utf-8 -*-
"""delete the index 
python3 deleteIndex.py --ES_host 98.11.56.20 --ES_port 10092 --index_name test_1
python3 deleteIndex.py --index_name test_1
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

import numpy as np
import scipy.signal

class delete_index(Operator):
    def __init__(self):
        super(delete_index, self).__init__()
        self.parse_args()

    def parse_args(self):
        # add my own args
        self._parser.add_argument('--ES_host', type=str,
            default="98.11.56.20",
            help='the ES ip or hostname')

        self._parser.add_argument('--ES_port', type=str,
            default="10092",
            help='the ES port')            

        self._parser.add_argument('--index_name', type=str,
            default="test_*",
            help='the ES index to delete')

        # always call super at the end of parse_args
        super(delete_index, self).parse_args()


    def main(self):
        try :
            print ("---------------deleting the ES index",self._flags.index_name,"-----------")
            es=Elasticsearch(host=self._flags.ES_host, port=self._flags.ES_port)   
            es.indices.delete(index=self._flags.index_name, ignore=[400, 404])
            print ("-----------------------Successfully------------------------------------------")
        except Exception as e :
            print (e)
            pass    

if __name__=="__main__":
    period = delete_index()
    period.main()
