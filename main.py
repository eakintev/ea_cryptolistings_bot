import argparse
import os
import sys
import yaml
from pprint import pprint
from utils import Workflow, Bot


def parse_arguments():
    """
    Parses command line argument

    Returns:
        string: path to configuration file
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config_path', help='absolute or relative path'
                                                    ' to configuration file')
    args = parser.parse_args()
    if args.config_path:
        config_path = args.config_path
    else:
        script_path = os.path.realpath(__file__)
        config_path = os.path.join(os.path.split(script_path)[0], 'config.yml')

    return config_path


def parse_config(config_path):
    """
    Parses configuration file

    Returns:
        dict: dictionary with configuration data
    """
    try:
        with open(config_path) as f:
            config = yaml.load(f)
    except FileNotFoundError:
        print(f'Configuration file not found at {os.path.realpath(config_path)}')
        sys.exit()

    return config


if __name__ == '__main__':
    CONFIG_PATH = parse_arguments()
    CONFIG = parse_config(CONFIG_PATH)
    pprint(CONFIG)

    proxies = None
    # proxies = {'https': "socks5://localhost:9150"}  # bypass blocking using Tor for local testing
    bot = Bot(CONFIG, proxies=proxies)

    # infinite run, default sleep_time 2 sec
    # upbit API is slow
    exchanges = [Workflow(exchange, CONFIG, bot) for exchange in CONFIG['exchanges']]
    for exchange in exchanges:
        exchange.start()
