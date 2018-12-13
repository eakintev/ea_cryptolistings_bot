import argparse
import os
import yaml
from utils import ConnectorGet, AdapterGet, Workflow, repeat_on_exception


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
        print('Configuration file not found')

    return config


if __name__ == '__main__':
    CONFIG_PATH = parse_arguments()
    CONFIG = parse_config(CONFIG_PATH)
    print(CONFIG)
    
    # infinite run for one exchange with default 2 sec sleep
    bittrex = Workflow('bittrex', CONFIG)
    bittrex.run()
