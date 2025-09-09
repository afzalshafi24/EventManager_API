import logging
import sys
import os
from fastapi import APIRouter

from . import db_handler, alert_queue, eClient
from .models import eventRequest, sparkRequest


#Create Logging Capability
logger = logging.getLogger(__name__)

event_mgr_router = APIRouter()

#Create a root endpoint EventManager
@event_mgr_router.get("/api")
async def read_root() -> dict:
    return {"message": "Welcome to the Event_Manager_API"}

# Define a POST endpoint for Event Submission
@event_mgr_router.post("/event/submit")
async def store_metric_alert(metric_data: eventRequest) -> dict: 
    '''
    Post Request to insert event into database
    Inputs:
        Post Request Message in JSON with the following fields:
        scid: int
        event_time: str
        event_rule_id: int
        event_name: str
        event_rule: str
        event_src: str
        spark_script: Optional[str]
        gem_full_path: Optional[str]
    Outputs:
        Dictionary with return message
    '''
    logger.info(f"POST Request for {metric_data.event_src} Event received with SCID: {metric_data.scid} and metric: {metric_data.event_name}")
    
    #Append alert queue with metric data
    alert_queue.append(metric_data)

    return {"message": f"Received item with scid: {metric_data.scid} and metric: {metric_data.event_name}"}

        
@event_mgr_router.get("/get_all_data")
async def get_all_data() -> dict:
    '''
    Get Request to get all data from all tables in database
    Inputs:
        N/A
    Outputs:
        Dictionary of Query Results or Errors
    '''
    try:
        #Method to get all data from DB Handler
        data = db_handler.get_all_data()
        return {"data" : data}
    
    except Exception as e:
        logger.error(f"No Data Availible due to {e}")
        return {"data": f"No Data Availible due to {e}"}
    
@event_mgr_router.get("/get_event_data")
async def get_event_data(scid:int, event_name:str) -> dict:
    '''
    Endpoint to get scid and event specific data
    Inputs:
        scid(integer) - payload id of interest to query
        event_name(string) - specific event name to query
    Outputs:
        Dictionary of Query Results or Errors
    '''
    try:
        data = db_handler.get_event_data(scid, event_name)
        return {"data" : data}
    
    except Exception as e:
        logger.error(f"No Data Availible due to {e}")
        return {"data": f"No Data Availible due to {e}"}
    
#Endpoint Analysis from SPARK
@event_mgr_router.post("/spark_endpoint")
async def get_spark_request(spark_request: sparkRequest) -> None:
    '''
    Post Request from SPARK to update event entry for given ID
    Inputs:
        Post Request Message in JSON with the following fields:
        job_id: int
        url: str
        spark_id: int
    Outputs:
        N/A
    '''
    logger.info(f'Getting Request from SPARK {spark_request}')

    #Update SPARK Report ID
    db_handler.update_column_by_id(spark_request.job_id, 'spark_report_id', spark_request.spark_id)
    

@event_mgr_router.on_event("shutdown")
async def shutdown_event() -> None:
    '''
    Endpoint to shutdown EventManager_API and to stop the threads gracefully
    Inputs:
        Keyboard Interrupt (Ctrl+C)
    Outputs:
        N/A
    '''
    global KEEP_POLLING
    logger.info(f'Shutting Down EventManager_API Application')
    alert_queue.append(None)  # Signal the thread to exit
    KEEP_POLLING = False
    eClient.stop()
    logger.info('Stopping Eurkea Client')
