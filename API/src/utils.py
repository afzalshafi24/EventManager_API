import os
import sys
import logging 
from time import sleep

from . import KEEP_POLLING, DB_POLL_LIST, DB_POLL_RATE, db_handler, SPARK_ENDPOINT, SPARK_URI, alert_queue, DATA_QUEUE_POLL_RATE
from .SPARK_Manager import spark_mgr

#Create Logging Capability
logger = logging.getLogger(__name__)


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
def data_ingestion_processing():
    #Constant Loop to check the queue for new events
    logger.info(f'Starting Data Ingestion Porcessing')
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
        sleep(DATA_QUEUE_POLL_RATE)