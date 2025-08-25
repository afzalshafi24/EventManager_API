
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from collections import deque
from time import sleep
import threading
import os
import logging 
from pathlib import Path
from logging.handlers import RotatingFileHandler
import sys
import requests 
import json 
from pydantic import BaseModel
from typing import Optional

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB_Handler.DB_Handler import DB_Handler
from API.SPARK_Manager import spark_mgr

#Open config file and extract params:
cfg_file = os.path.join(os.path.dirname(__file__), 'api_config.json')


#Read in JSON config file
with open(cfg_file, 'r') as file:
    #Load the JSON file
    cfg_data = json.load(file)

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

#Set set polling flag = True
KEEP_POLLING = True

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

# Define a Pydantic model for the request body for Spark Requests
class SparkRequest(BaseModel):
    job_id: int
    url: str
    spark_id: int

# Create an instance of the FastAPI class
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=ORGINS,  # Allows specified origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allows all headers
)

#Initialize Event Alert Queues
alert_queue = deque()

#Check to see if logging path exist , if not make one
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_name = Path(os.path.join(LOG_DIR, LOG_FILENAME))

# Configure logging
log_handler = RotatingFileHandler(log_name.as_posix(), maxBytes=10*1024*1024, backupCount=5)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
 
logging.basicConfig(level=logging.INFO, handlers=[log_handler, logging.StreamHandler()])


#Get Uvicorn Logger 
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(logging.INFO)
uvicorn_logger.addHandler(log_handler)
logger = logging.getLogger(__name__)

logger.info(f'Using config file {cfg_file}')

#Report config to log
logger.info(f'Starting Application on http://{IP}:{PORT}')
logger.info(f'Using DB address : {DB_URL}')

#Config and initialize Database
db_handler = DB_Handler(DB_URL)



def data_polling_processing():
    #Process to look for new events being stored

    logger.info(f'Starting Data Polling Porcessing for given sources {DB_POLL_LIST} polling for new events every {DB_POLL_RATE} seconds')

    while KEEP_POLLING:
        for src in DB_POLL_LIST:
            new_events = db_handler.find_new_events(src)
            
            for event in new_events:
                #Format New Event Message to send to SPARK
                new_event_msg = {'scid': event.scid, 
                                                            'start_time': event.event_time.strftime("%Y-%m-%d %H:%M:%S"),  
                                                            'metric': event.event_name, 
                                                            'source': event.event_src, 
                                                            'job_id': event.unique_index, 
                                                            'endpoint': SPARK_ENDPOINT}

                #Send Job to SPARK
                try:
                    spark_mgr(SPARK_URI,new_event_msg)
                    #Update SPARK Report ID
                    db_handler.update_column_by_id(event.unique_index, 'spark_report_id', -1)
                except Exception as e:
                    logger.error(f'Issue with SPARK Manager: {e}')

        sleep(DB_POLL_RATE)

#Function to process data in the queue 
def process_queue_data():
    #Constant Loop to check the queue for new events
    while True:
        if len(alert_queue) > 0:
            data_2_insert = alert_queue.popleft()
            if data_2_insert is None : 
                
                logger.info('Application is closing , shutting down thread')
                break
            elif data_2_insert.event_name.lower() == "heartbeat":
                logger.info(f'Storing {data_2_insert.source} Heartbeat Data for SCID {data_2_insert.scid}')

                try:
                    db_handler.store_heartbeat(data_2_insert)
                except Exception as e:
                   logger.error(f'Issues storing {data_2_insert.event_src} Heartbeat Data for SCID {data_2_insert.scid} due to {e}')
            else:
                #Store Data into database and send jobs to SPARK
                try:
                    logger.info(f'Storing {data_2_insert}')
                    
                    #Store new event in Database
                    db_handler.store_data(data_2_insert)

                    #Get Lastest ID for SPARK Manager
                    latest_id = db_handler.get_latest_id(data_2_insert.event_src)

                    #Format New Event Message to send to SPARK
                    new_event_msg = {'scid': data_2_insert.scid, 
                                                              'start_time': data_2_insert.event_time,  
                                                              'metric': data_2_insert.event_name, 
                                                              'source': data_2_insert.event_src, 
                                                              'job_id': latest_id, 
                                                              'endpoint': SPARK_ENDPOINT}
                    #Send Job to SPARK
                    spark_mgr(SPARK_URI,new_event_msg)

             
                except Exception as e:
                    logger.error(f'Issues with Storing {data_2_insert} : {e}')
        else:
            print('No data in Queue')
        sleep(3)



# Define a POST endpoint for Event Submission
@app.post("/event/submit")
async def store_metric_alert(metric_data: Event_Request):
    
    logger.info(f"POST Request for ASTRA Event received with SCID: {metric_data.scid} and metric: {metric_data.event_name}")
    
    #Append alert queue with metric data
    alert_queue.append(metric_data)

    return {"message": f"Received item with scid: {metric_data.scid} and metric: {metric_data.event_name}"}


@app.get("/get_data")
async def get_metric(scid: str, metric:str, table_name:str):
    print(f'Getting Data for scid {scid} for metric {metric} and table name {table_name}')
    

    try:
        #Method to grab scid specific data from a database
        data = db_handler.get_data(scid, metric, table_name)
        return {f"data" : data}
    except Exception as e:
        return {"data": f"No Data Availible with SCID: {scid}, METRIC: {metric} in {table_name} table"}
        
@app.get("/get_all_data")
async def get_all_data():
    #Get Request to get all data from all tables in database

    try:
        #Method to get all data from DB Handler
        data = db_handler.get_all_data()
        return data
    
    except Exception as e:
        return {"data": f"No Data Availible due to {e}"}

@app.get("/get_table")
async def get_table_data():
    #Endpoint to get table data from database
    return db_handler.get_table()

@app.get("/get_dataframe")
async def get_dataframe(table_name:str):
    print('Getting Request For DataFrame...')
    df = db_handler.get_table_as_dataframe(table_name)
    return {f"data" : df.to_dict(orient='dict')}

@app.get("/get_unique_db_vals")
async def get_unique_db_vals(col_name:str, table_name:str):
    #Get Unique values in database name
    return {f"data" : db_handler.get_unique_elements(col_name, table_name)}

@app.get("/db/heartbeat_info")
async def get_heartbeat_info(table_name:str):
    #Endpoint to get heartbeat info for a specific Application
    return db_handler.get_heartbeat_info(table_name)

#Endpoint Analysis from SPARK
@app.post("/spark_endpoint")
async def get_spark_request(spark_request: SparkRequest):
    logger.info(f'Getting Request from SPARK {spark_request}')

    #Update SPARK Report ID
    db_handler.update_column_by_id(spark_request.job_id, 'spark_report_id', spark_request.spark_id)
    
# Endpoint to stop the processing thread gracefully
@app.on_event("shutdown")
async def shutdown_event():
    global KEEP_POLLING
    
    alert_queue.append(None)  # Signal the thread to exit
    KEEP_POLLING = False

    data_ingestion_thread.join()  # Wait for the thread to finish
    logger.info("Data Ingestion Thread Sucessfully Closed")
    
    data_polling_thread.join()
    logger.info("Data Polling Thread Sucessfully Closed")

# Run the app with: uvicorn app:app --reload

if __name__ == "__main__":
    
    # Start the queue processing thread
    data_ingestion_thread = threading.Thread(target=process_queue_data, daemon=True)
    data_ingestion_thread.start()

    data_polling_thread = threading.Thread(target=data_polling_processing, daemon=True)
    data_polling_thread.start()

    import uvicorn
    uvicorn.run(app, host=IP, port=PORT, log_level="info")