import pymongo
from config.app_config import env_config
import urllib.parse

cfg = env_config()
mongo_host = cfg['MongoHost']
mongo_port = cfg['MongoPort']
mongo_username = urllib.parse.quote_plus(cfg['MongoUsername'])
mongo_password = urllib.parse.quote_plus(cfg['MongoPassword'])
MONGO_SERVER = f"mongodb://{mongo_username}:{mongo_password}@{mongo_host}"
if mongo_port is not None and mongo_port != '':
    MONGO_SERVER = f"{MONGO_SERVER}:{mongo_port}"
client = None

def init_mongo_client():
    global client
    client = pymongo.MongoClient(MONGO_SERVER)
    return client

def get_mongo_client():
    return client