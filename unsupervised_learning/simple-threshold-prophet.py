# python3 rule-any-prophet.py --es_host 192.168.0.21 --es_port 9900 --input-name mobile-mbank-log-2018.01 --reader_module lib.reader --reader_function aggs_avg_duration_by_ts

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
#import traceback
# framework
import sys
sys.path.append('..')
from Operator import Operator
# local dependencies
sys.path.append('./lib')
from detection import simple_threshold_model
from writer import writer_bulk
class Rule_Prophet(Operator):
    def __init__(self):
        super(Rule_Prophet, self).__init__()
        self.parse_args()
        self.initWatchdog()  
     

    def parse_args(self):
        # add my own args
        self._parser.add_argument('--upper_bound', type=int,
            default=1000,
            help='Upper bound rule')

        self._parser.add_argument('--lower_bound', type=int,
            default=10,
            help='Lower bound rule')

        self._parser.add_argument('--direction', type=str,
            default="both",
            help='alter direction')


        self._parser.add_argument('--metric_name', type=str,
            default='alert',
            help='Output metric name')

        # always call super at the end of parse_args
        super(Rule_Prophet, self).parse_args()

    def read_time_range_data(self, start_window, end):
        self.df = self.call(
            self._flags.reader_module,
            self._flags.reader_function,
            [],
            {
                'es_host': self._flags.es_host,
                'es_port': self._flags.es_port,
                'data_index': self._flags.input_name,
                'start': start_window,
                'end': end,
                "field":self._flags.field
            }
        )
    def write_alert(self):
        writer_bulk(
            self._flags.es_host,
            self._flags.es_port,
            self.df,
            self._flags.output_name     
        )
    def detection(self,output_start):
        self.df = simple_threshold_model(
                self.df,         
                self._flags.direction,
                self._flags.metric_name,
                output_start,
                self._flags.upper_bound,
                self._flags.lower_bound                
        )

    def batch(self, start_window, end,output_start):
        self.read_time_range_data(start_window, end)
        self.detection(output_start)
        self.write_alert()

    def loop(self):
        while True:
            self.watchdog() 
            
            print ("check  the  data  at ",(datetime.datetime.utcfromtimestamp(time.time())+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),"........................")
            # get last computed ts
            client = Elasticsearch(
                host=self._flags.es_host,
                port=self._flags.es_port
            )

            # get range to compute
            
            start = 0
            output_start=0
            start_window=0
            try:
                resp = client.search(
                    index =self._flags.output_name,
                    body = {
                        "size": 0,
                        "aggs": {
                            "latest_ts": {
                                "max": { "field": "createdAt" }
                            }
                        }
                    }
                )
                start = resp['aggregations']['latest_ts']['value']
                output_start=start +60000
                start_window=int(start-self._flags.loop_window_minutes*60000)         #start_window=int(end-self._flags.loop_window_minutes*60000)               
            except (Exception):
                pass
            except (KeyboardInterrupt):
                print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                sys.exit(0)         

            #end = int((time.time() // 60) * 60000)
            end = start+60000  #next  minute
            try:
                resp = client.search(
                    #index = '%s-rule' % self._flags.input_name,
                    index = self._flags.input_name,
                    body = {
                        "size": 0,
                        "aggs": {
                            "latest_ts": {
                                "max": { "field": "@timestamp" }
                            }
                        }
                    }
                )
                end = resp['aggregations']['latest_ts']['value']                
                end=int((end // 60000) * 60000)   # the minute of end time                 
            except (Exception):
                pass    
            except (KeyboardInterrupt):
                print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                sys.exit(0)                               
            #print ("read end:",datetime.datetime.utcfromtimestamp(end/1000).strftime("%Y-%m-%d %H:%M:%S"))       
            #print ("read start:",datetime.datetime.utcfromtimestamp(start_window/1000).strftime("%Y-%m-%d %H:%M:%S"))
            #print ("output start:",datetime.datetime.utcfromtimestamp(output_start/1000).strftime("%Y-%m-%d %H:%M:%S"))

            if start != end - 60000:  
                try:
                    print("start a new detection at ",(datetime.datetime.utcfromtimestamp(time.time())+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),"........................")          
                    self.read_time_range_data(start_window, end)  #@timestamp is millsecond，lt or lte dont produce the grave difference
                    self.detection(output_start)
                    self.write_alert()
                    print("finish the detection at ",(datetime.datetime.utcfromtimestamp(time.time())+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),"........................")          
                except (KeyboardInterrupt):
                    print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                    sys.exit(0)
                except (Exception):
                    print (Exception)
                    #traceback.print_exc()
            try:
                time.sleep(self._flags.loop_interval / 1000)
            except (KeyboardInterrupt):
                    print ("------You pressed Ctrl+C！The loop is interrupted ")
                    sys.exit(0)
if __name__=="__main__":
    prophet = Rule_Prophet()

    '''
    prophet.batch(
        '2018-01-29T12:00:00',
        '2018-01-29T12:59:00'
    )
    '''

    prophet.loop()
