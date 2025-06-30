
from fastapi import FastAPI

from collections import deque
from time import sleep
import threading
import configparser
import os
import logging 
import sys
from api_config import LOG_DIR, LOG_FILENAME, DB_FILE, IP, PORT, SPARK_URI, SPARK_ENDPOINT, ECQL_Request, SparkRequest, ASTRA_Request
import requests 

# Add parent directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from DB_Handler.DB_Handler import DB_Handler



# Create an instance of the FastAPI class
app = FastAPI()

#Initialize ECQL Alert Queues
alert_queue = deque()

#Check to see if logging path exist , if not make one
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_name = os.path.join(LOG_DIR, LOG_FILENAME)
# Configure logging with more detailed format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(log_name)
    ]
)   

logger = logging.getLogger(__name__)

#Config and initialize Database
print(f'Using DB file : {DB_FILE}')
db_handler = DB_Handler(DB_FILE)
db_handler.check_database()

#Function to process data in the queue 
def process_queue_data():
    while True:
        if len(alert_queue) > 0:
            data_2_insert = alert_queue.popleft()
            if data_2_insert is None : 
                
                logger.info('Application is closing , shutting down thread')
                break
            else:
                #Store Data into database and send jobs to SPARK
                try:
                    logger.info(f'Storing {data_2_insert}')
                    #Store Database
                    db_handler.store_data(data_2_insert)
                    latest_id = db_handler.get_latest_id(data_2_insert.source)
                
                    #Send Job to SPARK
                    print(data_2_insert)


                    response = requests.post(SPARK_URI, json={'scid': data_2_insert.scid, 
                                                              'start_time': data_2_insert.t,  
                                                              'metric': data_2_insert.metric_name, 
                                                              'source': data_2_insert.source, 
                                                              'job_id': latest_id, 
                                                              'endpoint': f"{SPARK_ENDPOINT}/{data_2_insert.source}"}) 
                except Exception as e:
                    logger.info(f'Issues with Storing {data_2_insert} : {e}')
        else:
            print('No data in Queue')
        sleep(3)



# Define a POST endpoint for ECQL
@app.post("/ECQL_store")
async def store_metric_alert(metric_data: ECQL_Request):
    
    print(f"Received item with SCID: {metric_data.scid} and metric: {metric_data.metric_name}")
    logger.info(f"Received item with SCID: {metric_data.scid} and metric: {metric_data.metric_name}")
    
    #Append alert queue with metric data
    alert_queue.append(metric_data)

    return {"message": f"Received item with scid: {metric_data.scid} and metric: {metric_data.metric_name}"}

# Define a POST endpoint for ECQL
@app.post("/ASTRA_store")
async def store_metric_alert(metric_data: ASTRA_Request):
    
    print(f"Received item with SCID: {metric_data.scid} and metric: {metric_data.metric_name}")
    logger.info(f"Received item with SCID: {metric_data.scid} and metric: {metric_data.metric_name}")
    
    #Append alert queue with metric data
    alert_queue.append(metric_data)

    return {"message": f"Received item with scid: {metric_data.scid} and metric: {metric_data.metric_name}"}

@app.get("/get_data")
def get_metric(scid: str, metric:str, table_name:str):
    print(f'Getting Data for scid {scid} for metric {metric} and table name {table_name}')
    #Method to grab scid specific data from a database
    return {f"data" : db_handler.get_data(scid, metric, table_name)}

@app.get("/get_dataframe")
async def get_dataframe(table_name:str):
    print('Getting Request For DataFrame...')
    df = db_handler.get_table_as_dataframe(table_name)
    return {f"data" : df.to_dict(orient='dict')}

@app.get("/get_unique_db_vals")
async def get_unique_db_vals(col_name:str, table_name:str):
    #Get Unique values in database name
    return {f"data" : db_handler.get_unique_elements(col_name, table_name)}

@app.get("/get_table_names")
async def get_db_table_names():
    #Get Table Names from database
    return {f"data" : db_handler.get_table_names()}

#ECQL Endpoint for ECQL Analysis from SPARK
@app.post("/spark_endpoint/ECQL")
def get_spark_request(spark_request: SparkRequest):
    print(f'Getting Request from SPARK {spark_request}')

    db_handler.update_url_by_id(spark_request.job_id, spark_request.url, 'ECQL')

#ECQL Endpoint for ASTRA Analysis from SPARK
@app.post("/spark_endpoint/ASTRA")
def get_spark_request(spark_request: SparkRequest):
    print(f'Getting Request from SPARK {spark_request}')

    db_handler.update_url_by_id(spark_request.job_id, spark_request.url, 'ASTRA')

# Endpoint to stop the processing thread gracefully
@app.on_event("shutdown")
def shutdown_event():
    alert_queue.append(None)  # Signal the thread to exit
    thread.join()  # Wait for the thread to finish

# Run the app with: uvicorn app:app --reload

if __name__ == "__main__":
    
    # Start the queue processing thread
    thread = threading.Thread(target=process_queue_data ,daemon=True)
    thread.start()
    import uvicorn
    uvicorn.run(app, host=IP, port=PORT)