import argparse
import importlib
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from sys import exit
from os import system

class Operator:
    def __init__(self):
        self._parser = argparse.ArgumentParser()
        self._flags = []

    @property
    def flags(self):
        return self._flags

    def call(self, module_name, function_name, args, kwargs):
        mod = importlib.import_module(module_name)
        func = getattr(mod, function_name)

        return func(*args, **kwargs)

    def parse_args(self):
        self._parser.add_argument('--input_type', type=str, default='ES_Index',
            choices=['MongoDB_Collection', 'Redis_Channel', 'NSQ_Queue', 'ES_Index'],
            help='Data input method.')

        self._parser.add_argument('--input_name', type=str, default='input',
            help='Data input name.')

        self._parser.add_argument('--output_type', type=str, default='ES_Index',
            choices=['MongoDB_Collection', 'Redis_Channel', 'NSQ_Queue', 'ES_Index'],
            help='Data output method')

        self._parser.add_argument('--output_name', type=str, default='output',
            help='Data output name.')

        self._parser.add_argument('--es_host', type=str, default='127.0.0.1',
            help='Elasticsearch host')

        self._parser.add_argument('--es_port', type=int, default=9200,
            help='Elasticsearch port')

        self._parser.add_argument('--mongo_host', type=str, default='127.0.0.1',
            help='MongoDB host')

        self._parser.add_argument('--mongo_port', type=int, default=27017,
            help='MongoDB port')

        self._parser.add_argument('--test', action='store_true',
            help='Test switch')

        self._parser.add_argument('--debug', action='store_true',
            help='Debug switch')

        self._parser.add_argument('--reader_module', type=str,
            default='lib.reader',
            help='Reader package and module name')

        self._parser.add_argument('--reader_function', type=str,
            default='read',
            help='Reader function name')

        self._parser.add_argument('--batch', action='store_true',
            help='Batch or loop mode')

        self._parser.add_argument('--loop_interval', type=int,
            default=1,
            help='Execute interval for loop mode')

        self._parser.add_argument('--watchdog_threshold', type=int,
            default=90,
            help='Shutdown threshold for watchdog')

        self._parser.add_argument('--loop_window_minutes', type=int,
            default=1440,
            help='calculate the data for the  time period ')

        self._parser.add_argument('--field', type=str,
            default='@timestamp',
            help='aggregate the field of ES index')

        self._parser.add_argument('--task_name', type=str,
            default='task',
            help='task name from prophet-server')

        self._flags, unparsed = self._parser.parse_known_args()

        
    def test(self):
        print('This is the operator test, please override in your instance.')

    def initWatchdog(self):
        self._mongoClient = MongoClient(self._flags.mongo_host, self._flags.mongo_port)

    def watchdog(self):
        db = self._mongoClient['prophet-server']
        collection = db.watchdogs
        watchdog = collection.find_one()
        updated_at = watchdog['updatedAt']

        tz_utc = timezone(timedelta(hours=0))
        updated_at = updated_at.replace(tzinfo=tz_utc)

        ts = updated_at.timestamp()
        now = datetime.now().timestamp()
        print('watchdog debug - my time: ', now, ' last feed: ', ts)

        if now - ts > self._flags.watchdog_threshold:
            print('watchdog killing process - my time: ', now, ' last feed: ', ts)
            # call pm2 to delete myself
            system('pm2 delete {0}'.format(self._flags.task_name))
            exit()
