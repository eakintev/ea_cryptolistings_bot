import json
import requests
import time
import datetime
import os


def repeat_on_exception(func):
    """Decorator to handle ConnectorGet exceptions."""
    def wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.Timeout:
                time.sleep(1)
            except requests.exceptions.ConnectionError:
                time.sleep(5)
    return wrapper


class ConnectorGet():
    """
    Class for get requests

    TODO: Better exceptions handling
    """
    def __init__(self, exchange, url,
                 headers={'Content-Type': 'application/json'}, timeout=1):
        self.exchange = exchange
        self.url = url
        self.headers = headers
        self.timeout = timeout

    @repeat_on_exception
    def get(self):
        req = requests.get(self.url, headers=self.headers, timeout=self.timeout)
        if req.status_code == 200:
            print(req.status_code)
            return {'data': req.json(), 'time': time.time()}
        else:
            req.raise_for_status()


class AdapterGet():
    """
    Class Adapter

    {'ETH-NPXS': '2018-12-06T17:40:19.817'} for bittrex
    {'BTC-NCASH': {timestamp of get response}} for upbit

    Method get_parsed_data returns tuple (parsed_data, string_data)
        - parsed_data - list of dicts FOR NOW:
                {'ETH-NPXS': '2018-12-06T17:40:19.817'} for bittrex
                {'BTC-NCASH': {timestamp of get response}} for upbit
        - string_data - list of strings 'BTCUSD is now at Bittrex \t (21:23:59 15.11.2018)'

    TODO: Time conversion, time format
    """
    def __init__(self, connector):
        self.connector = connector
        self.parsers = {
            'bittrex': lambda x: [{i['MarketName']: i['Created']} for i in x['data']['result']],
            'upbit': lambda x: [{i['market']: x['time']} for i in x['data']]
        }

    def get_parsed_data(self):
        # BTCUSD is now at Bittrex \t (21:23:59 15.11.2018)
        resp = self.connector.get()
        exchange = self.connector.exchange
        parser = self.parsers[exchange]
        parsed_data = parser(resp)
        string_data = [f'{k} is now at {exchange} \t ({v})' for i in parsed_data for k, v in i.items()]
        return parsed_data, string_data


class Workflow():
    """
    Base Workflow class

    Method run starts calling exchange API with given time interval
    """
    def __init__(self, exchange, config, sleep_time=2):
        self.exchange = exchange
        self.config = config
        self.json_path = os.path.join(self.config['data_folder'], exchange + '.json')
        self.sleep_time = sleep_time
        if os.path.exists(self.json_path):
            with open(self.json_path) as f:
                self.data = json.load(f)
        else:
            parsed_data, _ = self.run(loop=False)
            self.data = parsed_data
            with open(self.json_path, 'w') as f:
                json.dump(parsed_data, f)

    def run(self, loop=True):
        connector = ConnectorGet(self.exchange, self.config['exchanges'][self.exchange],
                                 {'Content-Type': 'application/json'}, timeout=1)
        adapter = AdapterGet(connector)

        if loop:
            while True:
                parsed_data, string_data = adapter.get_parsed_data()
                print(f'{len(parsed_data)} markets on {self.exchange}')
                time.sleep(self.sleep_time)
        else:
            return adapter.get_parsed_data()


class Archiver():
    """
    TODO
    """
    pass
