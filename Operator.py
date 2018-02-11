import argparse

class Operator:
    def __init__(self):
        self._parser = argparse.ArgumentParser()
        self._flags = []

    @property
    def flags(self):
        return self._flags

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

        self._flags, unparsed = self._parser.parse_known_args()

    def test(self):
        print('This is the operator test, please override in your instance.')
