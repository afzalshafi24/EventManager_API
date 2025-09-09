
#Python Imports
from collections import deque
import json
from .DB_Handler import DB_Handler
import logging
from eureka_client import EurekaClient
from pathlib import Path
import sys

#Initialize Logging
logger = logging.getLogger(__name__)


def read_event_mgr_config(cfg_file):
    #Reads in the EventManager config file
    #Open config file and extract params:
   
    #Read in JSON config file
    with open(cfg_file, 'r') as file:
        #Load the JSON file
        cfg_data = json.load(file)
    
    return cfg_data
s_path = Path.cwd().parent.parent
config_files = list(s_path.rglob('api_config.json'))

#Get Config file data
if len(config_files) > 0:
    cfg_file = config_files[0]
    cfg_data = read_event_mgr_config(cfg_file)
else:
    logger.error('No config file found, exiting application')
    sys.exit(1)

#Store Global Configuration Variables
LOG_DIR = cfg_data['LOG_DIR']
LOG_FILENAME = cfg_data['LOG_FILENAME']
DB_URL = cfg_data['DB_URL']
IP = cfg_data['IP']
PORT = cfg_data['PORT']
SPARK_URI = cfg_data['SPARK_URI']
ORGINS = cfg_data['ORGINS']
SPARK_ENDPOINT = rf'http://{IP}:{PORT}/spark_endpoint'
DB_POLL_LIST = cfg_data['DB_POLL_LIST']
DB_POLL_RATE = cfg_data['DB_POLL_RATE']
DATA_QUEUE_POLL_RATE = cfg_data['DATA_QUEUE_POLL_RATE']
EUREKA_FLG = cfg_data["EUREKA_FLG"]
EUREKA_SERVER = cfg_data["EUREKA_SERVER"]
APP_NAME = cfg_data["APP_NAME"]
INSTANCE_ID = cfg_data["INSTANCE_ID"] 
HEARTBEAT_INTERVAL = cfg_data["HEARTBEAT_INTERVAL"]



#Start the Eureka Server if FLG is set
if EUREKA_FLG:
    logger.info(f'Starting up Eureka Client for server location: {EUREKA_SERVER} with APP_NAME: {APP_NAME} and INSTANCE_ID: {INSTANCE_ID}')
    
    #Initialize Eureka Client
    try:
        eClient = EurekaClient(eureka_server=EUREKA_SERVER,
                                app_name=APP_NAME,
                                instance_id=INSTANCE_ID,
                                port=PORT,
                                heartbeat_interval=HEARTBEAT_INTERVAL)
        eClient.start()
        logger.info(f'Successfull Eureka Client bootup for server location: {EUREKA_SERVER} with APP_NAME: {APP_NAME} and INSTANCE_ID: {INSTANCE_ID}')
    except Exception as e:
        logger.error(f"Eureka Client configuration error: {e}")
        
else:
    logger.info("Eureka Client not configured")

#Initialize Event Alert Queues
alert_queue = deque()

#Initialize DB Handler Class
db_handler = DB_Handler(DB_URL)
db_handler.initialize_database()

#Initialize polling flag = True used for data polling processing
KEEP_POLLING = True

__all__ = ["ORGINS", "LOG_DIR", "LOG_FILENAME", "IP", "PORT", "cfg_file", "db_handler",
           "alert_queue","KEEP_POLLING", "DB_POLL_LIST", "DB_POLL_RATE", "SPARK_ENDPOINT", "SPARK_URI", 
           "EUREKA_FLG", "EUREKA_SERVER", "APP_NAME", "INSTANCE_ID", "HEARTBEAT_INTERVAL", "eClient"]