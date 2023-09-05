import configparser, os

cfg = None

def init_env_config():
    config = configparser.ConfigParser()
    config.read("config.ini")
    if 'CONTAINER_NAME' in os.environ.keys() and os.environ['CONTAINER_NAME'] in config.sections():
        cfg = config[os.environ['CONTAINER_NAME']]
    else:
        cfg = config['DEFAULT']
    return cfg

def env_config():
    if cfg is None:
        return init_env_config()
    return cfg