import requests
import logging


def spark_mgr(uri,new_event_msg):
    '''
    #Module to send jobs to spark based on event triggers from EventManager_API
    uri - address to SPARK API
    endpoint - address to EventManager_API to store SPARK Report IDs
    '''
    #Initialize Logging
    logger = logging.getLogger(__name__)

    #Log Spark Job Request
    logger.info(f'Sending EventManager_API job_id = {new_event_msg['job_id']} to SPARK')

    #Send Post Request to SPARK
    response = requests.post(uri, json=new_event_msg) 

     #Extract Response Status
    if not response.status_code == 200:
        logger.error(f"Error accessing SPARK: {response.status_code}, Response: {response.text}")
    
    
