import logging
import sys
import os
from fastapi import APIRouter

from . import db_handler, alert_queue
from .models import Event_Request, SparkRequest


#Create Logging Capability
logger = logging.getLogger(__name__)

event_mgr_router = APIRouter()

# Define a POST endpoint for Event Submission
@event_mgr_router.post("/event/submit")
async def store_metric_alert(metric_data: Event_Request):
    
    logger.info(f"POST Request for ASTRA Event received with SCID: {metric_data.scid} and metric: {metric_data.event_name}")
    
    #Append alert queue with metric data
    alert_queue.append(metric_data)

    return {"message": f"Received item with scid: {metric_data.scid} and metric: {metric_data.event_name}"}


@event_mgr_router.get("/get_data")
async def get_metric(scid: str, metric:str, table_name:str):
    print(f'Getting Data for scid {scid} for metric {metric} and table name {table_name}')
    

    try:
        #Method to grab scid specific data from a database
        data = db_handler.get_data(scid, metric, table_name)
        return {f"data" : data}
    except Exception as e:
        return {"data": f"No Data Availible with SCID: {scid}, METRIC: {metric} in {table_name} table"}
        
@event_mgr_router.get("/get_all_data")
async def get_all_data():
    #Get Request to get all data from all tables in database

    try:
        #Method to get all data from DB Handler
        data = db_handler.get_all_data()
        return data
    
    except Exception as e:
        return {"data": f"No Data Availible due to {e}"}

@event_mgr_router.get("/get_table")
async def get_table_data():
    #Endpoint to get table data from database
    return db_handler.get_table()

@event_mgr_router.get("/get_dataframe")
async def get_dataframe(table_name:str):
    print('Getting Request For DataFrame...')
    df = db_handler.get_table_as_dataframe(table_name)
    return {f"data" : df.to_dict(orient='dict')}

@event_mgr_router.get("/get_unique_db_vals")
async def get_unique_db_vals(col_name:str, table_name:str):
    #Get Unique values in database name
    return {f"data" : db_handler.get_unique_elements(col_name, table_name)}

@event_mgr_router.get("/db/heartbeat_info")
async def get_heartbeat_info(table_name:str):
    #Endpoint to get heartbeat info for a specific Application
    return db_handler.get_heartbeat_info(table_name)

#Endpoint Analysis from SPARK
@event_mgr_router.post("/spark_endpoint")
async def get_spark_request(spark_request: SparkRequest):
    logger.info(f'Getting Request from SPARK {spark_request}')

    #Update SPARK Report ID
    db_handler.update_column_by_id(spark_request.job_id, 'spark_report_id', spark_request.spark_id)
    
# Endpoint to stop the processing thread gracefully
@event_mgr_router.on_event("shutdown")
async def shutdown_event():
    global KEEP_POLLING
    
    logger.info(f'Shutting Down EventManager_API Application')
    alert_queue.append(None)  # Signal the thread to exit
    KEEP_POLLING = False
