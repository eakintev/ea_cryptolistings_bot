import os
import json
import requests
import datetime
import time
import pytz
from threading import Thread


# JSON_PARSERS return set of markets on exchange
JSON_PARSERS = {
    'bittrex': lambda x: {i['MarketName'] for i in x['data']['result']},
    'upbit': lambda x: {i['market'] for i in x['data']},
    'bithumb': lambda x: {i for i in x['data']['data'].keys() if i != 'date'},
    'kucoin': lambda x: {i['symbol'] for i in x['data']['data']},
    'poloniex': lambda x: set(x['data'].keys()),
    'binance': lambda x: {i['symbol'] for i in x['data']['symbols']}
}


def repeat_on_exception(func):
    """
    Decorator to handle ConnectorGet exceptions.
    """
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
            except requests.RequestException as e:
                t = 5
                print(e, retry_message(t), sep='\n')
                time.sleep(t)
    return wrapper


def threaded(fn):
    """
    To use as decorator to make a function call threaded.
    """
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
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
            return {'data': req.json(), 'timestamp(ms)': int(time.time()*1000)}
        else:
            req.raise_for_status()


class AdapterGet():
    """
    Class Adapter
    """
    def __init__(self, connector):
        self.connector = connector
        self.json_parsers = JSON_PARSERS

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

    def update_json(self, new_listings, ts):
        """
        Appends new listing to json file
        """
        with open(self.json_path) as f:
            data = json.load(f)

        with open(self.json_path, 'w') as f:
            for new_listing in new_listings:
                data.append({new_listing: ts})
            f.write(json.dumps(data, indent=4))


class Bot:
    """
    Telegram bot
    """
    def __init__(self, config, proxies=None):
        self.config = config
        self.proxies = proxies

    @threaded
    @repeat_on_exception
    def send_message(self, msg, chat_id):
        msg_base = f"https://api.telegram.org/bot{self.config['bot_token']}/sendMessage?chat_id={chat_id}&text="
        requests.post(msg_base+msg, proxies=self.proxies, timeout=2)


class Workflow(Thread):
    """
    Base Workflow class
    """
    def __init__(self, exchange, config, bot, sleep_time=2, verbose=1):
        """
        Args:
            exchange (string): name of exchange
            config (dict): dict with config data
            bot (Bot class instance): bot instance for message sending
            sleep_time (int): time between requests
            verbose (int):
                0 - no output
                1 - every minute
                2 - every request

        """
        Thread.__init__(self)
        self.exchange = exchange
        self.config = config
        self.json_path = os.path.join(self.config['data_folder'], exchange + '.json')
        self.sleep_time = sleep_time
        self.connector = ConnectorGet(self.exchange,
                                      self.config['exchanges'][self.exchange],
                                      {'Content-Type': 'application/json'},
                                      timeout=2)
        self.adapter = AdapterGet(self.connector)
        self.archiver = Archiver(self.json_path)
        self.bot = bot
        self.verbose = verbose
        self.start_time = datetime.datetime.now() 

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
        """
        self.current_minute = self.start_time.minute
        while True:
            new_parsed_data = self.adapter.parse_data()
            new_ts = new_parsed_data['timestamp(ms)']
            new_markets = new_parsed_data['markets']
            new_listings = new_markets - self.markets

            # if new market in API response
            if new_listings:
                # send messages first
                listings_str = ', '.join(new_listings)
                msg = self.adapter.get_string(listings_str, new_ts)
                for id_ in self.config['telegram_ids']:
                    handle = self.bot.send_message(msg, id_)
                    handle.join()
                print('\n' + msg + '\n')

                # update json
                self.archiver.update_json(new_listings, new_ts)

                self.markets = new_markets

            if self.verbose:
                dt = datetime.datetime.now()
                out_string = f'{len(new_markets)} markets on {self.exchange}\t {dt.strftime("%Y-%m-%d %H:%M:%S")}'
                if self.verbose == 1:
                    if dt.minute != self.current_minute:
                        self.current_minute = dt.minute
                        print(out_string)
                if self.verbose == 2:
                    print(out_string)

            time.sleep(self.sleep_time)
