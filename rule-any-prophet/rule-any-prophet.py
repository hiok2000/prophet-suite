# python3 rule-any-prophet.py --es_host 192.168.0.21 --es_port 9900 --input-name mobile-mbank-log-2018.01 --reader_module lib.reader --reader_function aggs_avg_duration_by_ts

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
from os.path import basename
import  pickle
import math
import datetime
import time
from elasticsearch import Elasticsearch

# framework
import sys
sys.path.append('..')
from Operator import Operator

# local dependencies
sys.path.append('./lib')

class Rule_Prophet(Operator):
    def __init__(self):
        super(Rule_Prophet, self).__init__()
        self.parse_args()

    def parse_args(self):
        # add my own args
        self._parser.add_argument('--upper_bound', type=int,
            default=1000,
            help='Upper bound rule')

        self._parser.add_argument('--lower_bound', type=int,
            default=10,
            help='Lower bound rule')

        self._parser.add_argument('--metric_name', type=str,
            default='alert',
            help='Output metric name')

        # always call super at the end of parse_args
        super(Rule_Prophet, self).parse_args()

    def read_time_range_data(self, start, end):
        self.df = self.call(
            self._flags.reader_module,
            self._flags.reader_function,
            [],
            {
                'es_host': self._flags.es_host,
                'es_port': self._flags.es_port,
                'data_index': self._flags.input_name,
                'start': start,
                'end': end
            }
        )

    def detection(self):
        self.df["anom_index"] = round(self.df["value"] / self._flags.upper_bound, 2)
        self.df.loc[self.df["value"] < self._flags.lower_bound, "anom_index"] = round(self._flags.lower_bound / self.df.loc[self.df["value"] < self._flags.lower_bound, "value"], 2)
        self.df.loc[(self.df["value"] < self._flags.upper_bound) & (self.df["value"] > self._flags.lower_bound), "anom_index"] = 1
        maximum = max(self.df["anom_index"])   # anom_index: 1 ~ max
        #self.df["serverity"] = [round(math.log(x, maximum), 4) * 100 for x in self.df["anom_index"]]
        self.df["serverity"] = 0
        self.df["level"] = "WARNING"
        self.df.loc[self.df["serverity"] == 0, "level"] = "NORMAL"
        self.df.loc[self.df["serverity"] > 50, "level"] = "ERROR"
        self.df["name"] = self._flags.metric_name
        self.df["timespan"] = 60000
        self.df["info"] = "value:" + self.df["value"].astype(str) + ", anom_index:" + self.df["anom_index"].astype(str)
        self.df["createdAt"] = self.df["ts"]
        self.df["updatedAt"] = self.df["ts"]
        
        self.df = self.df.filter(items = [
            "name",
            "serverity",
            "level",
            "timespan",
            "info",
            "createdAt",
            "updatedAt"
            ]
        )
        self.df = self.df.sort_values(by = "createdAt")

    def write_alert(self):
        print(self.df)

    def batch(self, start, end):
        self.read_time_range_data(start, end)
        self.detection()
        self.write_alert()

    def loop(self):
        while True:
            # get last computed ts
            client = Elasticsearch(
                host=self._flags.es_host,
                port=self._flags.es_port
            )

            # get range to compute
            start = 0
            try:
                resp = client.search(
                    index = '%s-rule' % self._flags.input_name,
                    body = {
                        "size": 0,
                        "aggs": {
                            "latest_ts": {
                                "max": { "field": "createdAt" }
                            }
                        }
                    }
                )
                start = resp['aggregations']['latest_ts']
            except (Exception):
                pass
                
            end = int((time.time() // 60) * 60000)
            if start != end:
                end = end - 60000
                self.read_time_range_data(start, end)
                self.detection()
                self.write_alert()

            time.sleep(self._flags.loop_interval / 1000)

if __name__=="__main__":
    prophet = Rule_Prophet()

    '''
    prophet.batch(
        '2018-01-29T12:00:00',
        '2018-01-29T12:59:00'
    )
    '''

    prophet.loop()
