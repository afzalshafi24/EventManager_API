#ECQL emulator to process batch scripts 
import pandas as pd
import subprocess
from time import sleep
from datetime import datetime
import sys
import random 
from numpy import isnan
import os
import json
import logging 
from pathlib import Path
from logging.handlers import RotatingFileHandler
import datetime
from pydantic import BaseModel
from typing import Optional

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB_Handler.DB_Handler import DB_Handler

# Define a Pydantic model for the request body for Event Alerts
class Event_Request(BaseModel):
    scid: int
    event_time: str
    event_rule_id: int
    event_name: str
    event_rule: str
    event_src: str
    spark_script: Optional[str]
    gem_full_path: Optional[str]


def ECQL_emulator(ecql_cfg, scid):
    #Open config file and extract params:
    cfg_file = os.path.join(os.path.dirname(__file__), 'ecql_sim_cfg.json')


    #Read in JSON config file
    with open(cfg_file, 'r') as file:
        #Load the JSON file
        cfg_data = json.load(file)

    LOG_DIR = os.path.join(os.path.dirname(__file__), 'logs')
    LOG_FILENAME = cfg_data['LOG_FILENAME']
    DB_URL = cfg_data['DB_URL']
    SLEEP_RANGE = cfg_data['SLEEP_RANGE']
    
    #Check to see if logging path exist , if not make one
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)

    log_name = Path(os.path.join(LOG_DIR, LOG_FILENAME))

    # Configure logging
    log_handler = RotatingFileHandler(log_name.as_posix(), maxBytes=10*1024*1024, backupCount=5)
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    
    logging.basicConfig(level=logging.INFO, handlers=[log_handler, logging.StreamHandler()])
    logger = logging.getLogger(__name__)

    #Extract data from cfg_data
    cfg_data = pd.read_csv(ecql_cfg, delimiter=';')
    num_of_events = len(cfg_data['#Event_Name'])
    
    #Config and initialize Database
    db_handler = DB_Handler(DB_URL)
    db_handler.initialize_database()

    while True:
        #Generate random sleep time
        sleep_time = random.randint(SLEEP_RANGE[0],SLEEP_RANGE[1])
        
        #Get Random Batch Script to run
        event_idx = random.randint(0, num_of_events-1)
        batch_file = cfg_data['#AppName'][event_idx]

        # Get the current date and time
        now = datetime.datetime.now()

        # Format the date and time
        event_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        if isnan(batch_file):
            logger.info('No batch file.')
            
        else:

            print(f'Executing batch file: {batch_file}')
            subprocess.run(f'{batch_file}')
        
        new_event = Event_Request(scid=scid,
                                  event_time=event_time,
                                  event_rule_id=event_idx,
                                  event_name=cfg_data['#Event_Name'][event_idx],
                                  event_rule = cfg_data['#EventRule'][event_idx],
                                  event_src='ecql',
                                  spark_script='',
                                  gem_full_path='C:\\vcid.pgem')
        
        logger.info(f'Adding ECQL New Event {new_event}')
        
        #Store data in database
        db_handler.store_data(new_event)

        sleep(sleep_time)



if __name__ == "__main__":
    
    #get inputs
    ecql_cfg = sys.argv[1]
    scid = sys.argv[2]

    #Start ECQL Emulator
    ECQL_emulator(ecql_cfg, scid)




