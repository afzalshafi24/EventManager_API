'''
Tool to simulate data to send to EventManager_API
python data_ingestion_sim.py

Dependencies
config.json - config file used with data_ingestion.py

'''


import requests
import sys
import datetime
import json
import random 
import time


def send_POST_request(scid, event_name, event_src, event_params, uri):
    #Module to send POST Request to EventManager_API

    # Get the current date and time
    now = datetime.datetime.now()

    # Format the date and time
    event_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    response = requests.post(uri, 
                                json={'scid': scid,
                                      'event_time': event_time,  
                                      'event_rule_id': event_params['event_rule_id'],
                                      'event_name': event_name,
                                      'event_rule': event_params['event_rule'],
                                      'event_src': event_src,
                                      'spark_script': event_params['spark_script'],
                                      'gem_full_path': r'C:/vcid.pgem'})

def data_ingestion_sim(cfg):
    #Simulate sending POST Request data to EventManager_API

    #Read in JSON config file
    with open(cfg, 'r') as file:
        #Load the JSON file
        cfg_data = json.load(file)
    
    #Get URI from config file
    uri = cfg_data['uri']

    #Get min and max sleep values
    min_sleep = cfg_data['sleep_range'][0]
    max_sleep = cfg_data['sleep_range'][1]

    while True:
        #Pick a random scid
        scid = random.choice(cfg_data['scids'])
        
        #Select Random Src
        event_src = random.choice(list(cfg_data['event_srcs']))

        #Seletc Random Event from Random Src
        event_name = random.choice(list(cfg_data['event_srcs'][event_src]))

        #Extract Event Params from cfg
        event_params = cfg_data['event_srcs'][event_src][event_name]

        #Send Post Request
        print(f"Event {event_name} detected in SCID {scid} selected from data source {event_src} ")
        send_POST_request(scid, event_name, event_src, event_params, uri)

        time.sleep(random.randint(min_sleep,max_sleep))

if __name__ == '__main__':
    cfg = sys.argv[1]
    data_ingestion_sim(cfg)
