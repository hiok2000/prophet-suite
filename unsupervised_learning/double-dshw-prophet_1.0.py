#!/usr/bin/python5
# add  a function in reader.py

#/home/voyager/windows/lz-deploy/prophet-suite/bin/python3 double-dshw-prophet_1.0.py  --metric_name  dshw  --agg_index  agg_test  --input_name  esb-in-log-*   --output_name  result_dshw  --es_host 98.11.56.20 --es_port 10092   --reader_module lib.reader  --reader_function totalcount_by_ts  --field  @timestamp  --loop_interval 30000  --loop_window_minutes   80640   --cf  3  --direction both --minimum 0  --smoothing_window_minutes 30  --watchdog_threshold 90 --mongo_port 27017 --mongo_host 98.11.56.20

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
sys.path.append('./lib')
from detection import double_dshw_model
from writer import writer_bulk
import clean
import dshw
from reader import query_by_ts


class Rule_Prophet(Operator):
    def __init__(self):
        super(Rule_Prophet, self).__init__()
        self.parse_args()
        self.initWatchdog() 

    def parse_args(self):
        # add my own args
        self._parser.add_argument('--cf', type=int,
            default=3,
            help='the confidence interval')

        self._parser.add_argument('--direction', type=str,
            default="both",
            help='alter direction')

        self._parser.add_argument('--minimum', type=int,
            default=0,
            help='minimum value')

        self._parser.add_argument('--metric_name', type=str,
            default='alert',
            help='Output metric name')

        self._parser.add_argument('--smoothing_window_minutes', type=int,
            default=30,
            help='smoothing_window')   

        self._parser.add_argument('--m', type=int,
            default=1440,
            help='the shorter period ')

        self._parser.add_argument('--m2', type=int,
            default=14400,
            help='the longer period ')

        self._parser.add_argument('--agg_index', type=str,   #new parametres:tasks :core_tux_queue_avg
            default="test",
            help='the agg result index ')



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
                'field': self._flags.field,
                'start': start_window,
                'end': end
            }
        )

    def write_alert(self,output_index):
        writer_bulk(
            self._flags.es_host,
            self._flags.es_port,
            self.df,
            #self._flags.output_name
            output_index
    
        )

    def detection(self,output_start):
        self.df = double_dshw_model(
                self.df,
                self._flags.cf,           
                self._flags.direction,
                self._flags.metric_name,
                output_start,
                self._flags.smoothing_window_minutes,
                self._flags.minimum,
                self._flags.m,
                self._flags.m2
        )
   

    # def batch(self, start_window, end,output_start):
    #     self.read_time_range_data(start_window, end)
    #     self.detection(output_start)
    #     self.write_alert()

    def loop(self):
        while True:
            self.watchdog() 
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
                    #index = '%s-rule' % self._flags.input_name,
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
                
                start = resp['aggregations']['latest_ts']['value']  #9:10  =[9:10:0,9:10:59]
                output_start=start+60000 
                start_window=int(start-self._flags.loop_window_minutes*60000)                
            except (Exception):
                pass
            except (KeyboardInterrupt):
                print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                sys.exit(0)                
            #end = int((time.time() // 60) * 60000)


            agg_start = 0
            try:
                resp = client.search(
                    #index = '%s-rule' % self._flags.input_name,
                    index =self._flags.agg_index,
                    body = {
                        "size": 0,
                        "aggs": {
                            "latest_ts": {
                                "max": { "field": "datetime" }
                            }
                        }
                    }
                )
                
                start1 = resp['aggregations']['latest_ts']['value']  #9:10  =[9:10:0,9:10:59]
                agg_start=int(((start1+60000)//60000)*60000)            
            except (Exception):
                pass
            except (KeyboardInterrupt):
                print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                sys.exit(0)      
#----min
            start2 = 0
            try:
                resp = client.search(
                    #index = '%s-rule' % self._flags.input_name,
                    index =self._flags.agg_index,
                    body = {
                        "size": 0,
                        "aggs": {
                            "first_ts": {
                                "min": { "field": "datetime" }
                            }
                        }
                    }
                )
                
                start2 = resp['aggregations']['first_ts']['value']  #9:10  =[9:10:0,9:10:59]
                start2 =int(start2)            
            except (Exception):
                pass
            except (KeyboardInterrupt):
                print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                sys.exit(0)     
            start_window = max(start_window,start2)

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
            print ("check  the  data  at ",(datetime.datetime.utcfromtimestamp(time.time())+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),"........................")

            #print ("start ,agg_start , output_start ,end ,start_window :"  ,start ,agg_start , output_start ,end ,start_window )


            if start != end - 60000:  
                try:
                    print("start a new detection at ",(datetime.datetime.utcfromtimestamp(time.time())+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),"........................")          
                    self.read_time_range_data(agg_start, end)  #@timestamp is millsecond，lt or lte dont produce the grave difference
                    
                    self.df["datetime"]=pd.to_datetime(self.df["datetime"])   
                    
                    output_index = self._flags.agg_index
                    self.write_alert(output_index)
                    

                    self.df = query_by_ts(self._flags.es_host,self._flags.es_port,self._flags.agg_index,start_window, end,field = "datetime")       #read the agg data
                    
                    self.detection(output_start)
                    output_index = self._flags.output_name
                    self.write_alert(output_index)
                    
                    print("finish the detection at ",(datetime.datetime.utcfromtimestamp(time.time())+datetime.timedelta(hours=8)).strftime("%Y-%m-%d %H:%M:%S"),"........................")          
                except (KeyboardInterrupt):
                    print ("-----You pressed Ctrl+C ！The loop is interrupted ")                
                    sys.exit(0)
                except Exception as e:
                    print (e)

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
