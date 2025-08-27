
#Python Imports
from collections import deque
import sys
import os
import json



# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from DB_Handler.DB_Handler import DB_Handler

def read_event_mgr_config(cfg_file):
    #Reads in the EventManager config file
    #Open config file and extract params:
    cfg_file = os.path.join(os.path.dirname(__file__), '../config/api_config.json')


    #Read in JSON config file
    with open(cfg_file, 'r') as file:
        #Load the JSON file
        cfg_data = json.load(file)
    
    return cfg_data

#Get Config file data
cfg_file = 'api_config.json'
cfg_data = read_event_mgr_config(cfg_file)

#Store Global Variables
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

#Initialize Event Alert Queues
alert_queue = deque()

#Initialize DB Handler Class
db_handler = DB_Handler(DB_URL)


#Initialize polling flag = True used for data polling processing
KEEP_POLLING = True


__all__ = ["ORGINS", "LOG_DIR", "LOG_FILENAME", "IP", "PORT", "cfg_file", "db_handler",
           "alert_queue","KEEP_POLLING", "DB_POLL_LIST", "DB_POLL_RATE", "SPARK_ENDPOINT", "SPARK_URI"]