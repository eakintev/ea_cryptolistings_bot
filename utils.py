import os
import json
import requests
import datetime
import time
import pytz
from threading import Thread


def repeat_on_exception(func):
    """Decorator to handle ConnectorGet exceptions."""
    def retry_message(time):
        return f'Sleep for {time} sec and retry'

    def wrapper(*args, **kwargs):
        while True:
            try:
                return func(*args, **kwargs)
            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError) as e:
                t = 1
                print(e, retry_message(t), sep='\n')
                time.sleep(t)
            except Exception as e:
                t = 5
                print(e, retry_message(t), sep='\n')
                time.sleep(t)
    return wrapper


class ConnectorGet():
    """
    Class for get requests to API

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
        """
        Makes GET request to API, retries if unsuccessful

        Returns:
            dict: {'data': json response from API,
                   'timestamp(ms): timestamp of response}
        """
        req = requests.get(self.url, headers=self.headers, timeout=self.timeout)
        if req.status_code == 200:
            # print(req.status_code)
            return {'data': req.json(), 'timestamp(ms)': int(time.time()*1000)}
        else:
            req.raise_for_status()


class AdapterGet():
    """
    Class Adapter
    """
    def __init__(self, connector):
        self.connector = connector
        # parsers return set of markets
        self.json_parsers = {
            'bittrex': lambda x: {i['MarketName'] for i in x['data']['result']},
            'upbit': lambda x: {i['market'] for i in x['data']}
        }

    def _convert_ts(self, ts, tz='Europe/Moscow'):
        """
        Converts UTC timestamp(ms) to date and time in given timezone

        Args:
            ts (int): timestamp(ms)
            tz (string): timezone

        Returns:
            string: date string, e.g. '21:23:59 15.11.2018 MSK'
        """
        ts = ts / 1000  # convert ms to s
        dt_string = (datetime.datetime.fromtimestamp(ts, tz=pytz.utc)
                                      .astimezone(pytz.timezone(tz))
                                      .strftime('%H:%M:%S %d.%m.%Y %Z'))
        return dt_string

    def parse_data(self):
        """
        Parses API json data from Connector

        Returns:
            dict: {'markets': set of markets,
                   'timestamp(ms)': ts_of_response}
        """
        response = self.connector.get()
        json_parser = self.json_parsers[self.connector.exchange]
        markets = json_parser(response)

        return {'markets': markets, 'timestamp(ms)': response['timestamp(ms)']}

    def get_string(self, market, ts):
        """
        Returns message with market and time of listing
        """
        return f'{market} is now at {self.connector.exchange} \t ({self._convert_ts(ts)})'


class Archiver():
    """
    Class that writes market data to file
    """
    def __init__(self, json_path):
        self.json_path = json_path

    def write_new(self, parsed_data):
        """
        Writes markets data from adapter to json file
        """
        json_data = [{market: parsed_data['timestamp(ms)']} for market in parsed_data['markets']]
        with open(self.json_path, 'w') as f:
            f.write(json.dumps(json_data, indent=4))

    def update_json(self, new_market, ts):
        """
        Appends new listing to json file
        """
        with open(self.json_path, 'r+') as f:
            data = json.load(f)
            data.append({new_market: ts})
            f.write(json.dumps(data, indent=4))


class Workflow(Thread):
    """
    Base Workflow class
    """
    def __init__(self, exchange, config, sleep_time=2):
        Thread.__init__(self)
        self.exchange = exchange
        self.config = config
        self.json_path = os.path.join(self.config['data_folder'], exchange + '.json')
        self.sleep_time = sleep_time
        self.connector = ConnectorGet(self.exchange,
                                      self.config['exchanges'][self.exchange],
                                      {'Content-Type': 'application/json'},
                                      timeout=1)
        self.adapter = AdapterGet(self.connector)
        self.archiver = Archiver(self.json_path)

        # Run GET request once and save data if json file doesn't exist
        if not os.path.exists(self.json_path):
            parsed_data = self.adapter.parse_data()
            self.archiver.write_new(parsed_data)

        # load markets data from json file
        with open(self.json_path) as f:
            json_data = json.load(f)
            self.markets = {k for d in json_data for k in d.keys()}

    def run(self):
        """
        Calling exchange API with given time interval sleep_time

        TODO: bot class
              sending multithreaded (async?) messages
              telegram restriction 30 msg/sec to different users
        """
        while True:
            new_parsed_data = self.adapter.parse_data()
            new_ts = new_parsed_data['timestamp(ms)']
            new_markets = new_parsed_data['markets']
            new_listings = new_markets - self.markets
            # if new market in API response
            if new_listings:
                # send messages here first
                for listing in new_listings:
                    msg = self.adapter.get_string(listing, new_ts)

                self.markets = new_markets
                self.archiver.update_json(new_market, new_ts)

            dt = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f'{len(new_markets)} markets on {self.exchange}\t {dt}')

            # print(new_markets)
            # proxies = {'https': "socks5://localhost:9150"}  # bypass blocking using Tor for local testing
            # for id_ in self.config['telegram_ids']:
            #     msg_base = f"https://api.telegram.org/bot{self.config['bot_token']}/sendMessage?chat_id={id_}&text="
            #     msg = self.adapter.get_string(list(new_markets)[0], new_ts)
            #     requests.post(msg_base+msg, proxies=proxies)

            time.sleep(self.sleep_time)
