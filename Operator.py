import argparse
import importlib

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
        self._parser.add_argument('--input-type', type=str, default='ES_Index',
            choices=['MongoDB_Collection', 'Redis_Channel', 'NSQ_Queue', 'ES_Index'],
            help='Data input method.')

        self._parser.add_argument('--input-name', type=str, default='input',
            help='Data input name.')

        self._parser.add_argument('--output-type', type=str, default='ES_Index',
            choices=['MongoDB_Collection', 'Redis_Channel', 'NSQ_Queue', 'ES_Index'],
            help='Data output method')

        self._parser.add_argument('--output-name', type=str, default='output',
            help='Data output name.')

        self._parser.add_argument('--es_host', type=str, default='127.0.0.1',
            help='Elasticsearch host')

        self._parser.add_argument('--es_port', type=int, default=9200,
            help='Elasticsearch port')

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
            default=30000,
            help='Execute interval for loop mode')

        self._flags, unparsed = self._parser.parse_known_args()

    def test(self):
        print('This is the operator test, please override in your instance.')
